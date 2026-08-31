// Copyright (C) 2026 Antony Stubbs and contributors

#include "sidecar.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <string>
#include <vector>

#include "error.h"

namespace parallelconsumer::proxy {
namespace {

/// The lifecycle channel's whole vocabulary: the proxy prints `port: <n>` and connects nothing else
/// to it.
constexpr const char* kPortLinePrefix = "port: ";

/// How many of the sidecar's own output lines are kept for the diagnostic. Bounded on purpose - see
/// Sidecar::recent_output.
constexpr std::size_t kTailLines = 40;

void close_if_open(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

}  // namespace

std::optional<std::uint16_t> Sidecar::parse_port_line(const std::string& line) {
    const std::string prefix(kPortLinePrefix);
    if (line.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }
    const std::string rest = line.substr(prefix.size());
    std::size_t first = rest.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return std::nullopt;
    }
    std::size_t last = rest.find_last_not_of(" \t\r\n");
    const std::string digits = rest.substr(first, last - first + 1);
    if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
        return std::nullopt;
    }
    const unsigned long parsed = std::stoul(digits);
    if (parsed == 0 || parsed > 65535) {
        return std::nullopt;
    }
    return static_cast<std::uint16_t>(parsed);
}

Sidecar::Sidecar(const ClientOptions& options) : path_(options.sidecar_path) {
    int stdin_pipe[2] = {-1, -1};
    int stdout_pipe[2] = {-1, -1};
    if (::pipe(stdin_pipe) != 0 || ::pipe(stdout_pipe) != 0) {
        throw SidecarError(std::string("could not create the lifecycle pipes: ") + std::strerror(errno));
    }

    // argv is built BEFORE the fork: between fork and exec only async-signal-safe calls are legal,
    // and allocating a vector of C strings is not one of them.
    std::vector<std::string> arguments;
    arguments.reserve(options.sidecar_args.size() + 1);
    arguments.push_back(options.sidecar_path);
    arguments.insert(arguments.end(), options.sidecar_args.begin(), options.sidecar_args.end());

    // The reference cannot be const, whatever cppcheck says: execv takes char* const[], and only the
    // NON-const std::string::data() overload returns a char*. A const reference here does not
    // compile, so the finding is a false positive rather than a style choice.
    std::vector<char*> argv(arguments.size() + 1, nullptr);
    std::transform(arguments.begin(), arguments.end(), argv.begin(),
                   // cppcheck-suppress constParameterReference
                   [](std::string& argument) { return argument.data(); });
    const bool null_stderr = options.sidecar_stderr == SidecarStderr::Null;

    pid_ = ::fork();
    if (pid_ == 0) {
        // The child. execv, NEVER a shell - see the class comment.
        ::dup2(stdin_pipe[0], STDIN_FILENO);
        ::dup2(stdout_pipe[1], STDOUT_FILENO);
        if (null_stderr) {
            const int null_fd = ::open("/dev/null", O_WRONLY);
            if (null_fd >= 0) {
                ::dup2(null_fd, STDERR_FILENO);
                ::close(null_fd);
            }
        }
        ::close(stdin_pipe[0]);
        ::close(stdin_pipe[1]);
        ::close(stdout_pipe[0]);
        ::close(stdout_pipe[1]);
        ::execv(argv[0], argv.data());
        ::_exit(127);
    }

    const int spawn_errno = errno;
    ::close(stdin_pipe[0]);
    ::close(stdout_pipe[1]);
    if (pid_ < 0) {
        ::close(stdin_pipe[1]);
        ::close(stdout_pipe[0]);
        throw SidecarError(path_ + " could not be started: " + std::strerror(spawn_errno));
    }
    lifecycle_fd_ = stdin_pipe[1];
    stdout_fd_ = stdout_pipe[0];

    // THE DRAIN RUNS FOR THE CHILD'S WHOLE LIFE, not just until the port line. A pipe nobody reads
    // fills up - 64 KiB on Linux, which a JVM at INFO reaches in seconds under load - and the
    // sidecar then stops mid-log-line and never returns, which reaches the application as a stalled
    // consumer with no error and nothing in any log.
    drain_ = std::thread([this] { drain_stdout(); });

    std::unique_lock<std::mutex> lock(mutex_);
    const bool announced = port_arrived_.wait_for(lock, options.connect_timeout, [this] {
        return announced_port_.has_value() || stdout_ended_;
    });
    if (announced_port_) {
        port_ = *announced_port_;
        lock.unlock();
        if (options.logger) {
            options.logger(LogLevel::Info, "sidecar " + path_ + " announced port " + std::to_string(port_));
        }
        return;
    }
    lock.unlock();
    const std::string tail = recent_output();
    stop(options.shutdown_grace);
    if (!announced) {
        throw TimeoutError("waiting " + std::to_string(options.connect_timeout.count()) +
                           "ms for the sidecar's port line. Its last output was:\n" + tail);
    }
    throw SidecarError("stdout ended before a 'port: <n>' line. Its last output was:\n" + tail);
}

Sidecar::~Sidecar() {
    // A destructor cannot throw, so whatever stop() reports is dropped here; the supported route is
    // Client::shutdown(), which calls stop() itself and reports what it says.
    stop(std::chrono::seconds(5));
}

void Sidecar::drain_stdout() {
    std::string buffer;
    char chunk[4096];
    for (;;) {
        const ssize_t read_bytes = ::read(stdout_fd_, chunk, sizeof(chunk));
        if (read_bytes > 0) {
            buffer.append(chunk, static_cast<std::size_t>(read_bytes));
            for (;;) {
                const std::size_t newline = buffer.find('\n');
                if (newline == std::string::npos) {
                    break;
                }
                record_line(buffer.substr(0, newline));
                buffer.erase(0, newline + 1);
            }
            continue;
        }
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    if (!buffer.empty()) {
        record_line(buffer);
    }
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        stdout_ended_ = true;
    }
    port_arrived_.notify_all();
}

void Sidecar::record_line(const std::string& line) {
    const std::lock_guard<std::mutex> lock(mutex_);
    tail_.push_back(line);
    while (tail_.size() > kTailLines) {
        tail_.pop_front();
    }
    if (!announced_port_) {
        // Scanned, not read once: the harness logs before its port line.
        if (const auto announced = parse_port_line(line)) {
            announced_port_ = announced;
            port_arrived_.notify_all();
        }
    }
}

std::string Sidecar::recent_output() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    std::string rendered;
    for (const auto& line : tail_) {
        rendered += "    ";
        rendered += line;
        rendered += "\n";
    }
    return rendered;
}

std::optional<std::string> Sidecar::stop(std::chrono::milliseconds grace) {
    close_if_open(lifecycle_fd_);  // the parent-death signal, and the reap

    std::optional<std::string> problem;
    if (pid_ > 0) {
        const auto deadline = std::chrono::steady_clock::now() + grace;
        bool exited = false;
        for (;;) {
            int status = 0;
            const pid_t waited = ::waitpid(pid_, &status, WNOHANG);
            if (waited == pid_ || (waited < 0 && errno == ECHILD)) {
                exited = true;
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (!exited) {
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
            problem = "did not exit within " + std::to_string(grace.count()) +
                      "ms of its lifecycle pipe closing, so it was killed";
        }
        pid_ = -1;
    }

    // The child is gone, so the write end of its stdout pipe is closed and the drain's read()
    // returns 0 on its own. JOIN BEFORE CLOSING, never the other way round: closing a descriptor a
    // thread is blocked reading frees that descriptor NUMBER for the next file this process opens,
    // and the drain would then be reading somebody else's file.
    if (drain_.joinable()) {
        drain_.join();
    }
    close_if_open(stdout_fd_);
    return problem;
}

}  // namespace parallelconsumer::proxy
