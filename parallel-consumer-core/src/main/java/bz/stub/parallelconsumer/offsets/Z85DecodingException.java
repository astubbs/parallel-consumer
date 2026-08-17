package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Thrown when a string cannot be decoded as {@link Z85Codec} output - an invalid character, an impossible
 * length, or a group that is not a canonical encoding of any byte sequence.
 * <p>
 * Deliberately <em>checked</em> (extends {@link Exception}, not {@link RuntimeException}): the strings this
 * codec is handed come from the broker, so a decode failure is an expected, recoverable input condition -
 * foreign or corrupt offset metadata - not a programming error. Callers must convert it to
 * {@link OffsetDecodingError} so it joins the existing foreign-metadata recovery path, exactly as a Base64
 * {@link IllegalArgumentException} does today. Making it unchecked would let a caller forget, and a corrupt
 * commit from another tool would then kill the poller instead of dropping the offset map.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class Z85DecodingException extends Exception {

    public Z85DecodingException(String message) {
        super(message);
    }

}
