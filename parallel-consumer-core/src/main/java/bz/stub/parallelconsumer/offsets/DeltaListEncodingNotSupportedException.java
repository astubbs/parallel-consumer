package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */


/**
 * Thrown when the {@link DeltaListEncoder} cannot encode the given data - the range to encode is longer than the
 * format's 4-byte {@code rangeLength} field can address.
 * <p>
 * As with its siblings, being an {@link EncodingNotSupportedException} is what makes
 * {@link OffsetSimultaneousEncoder} drop this encoder out of the competitive set rather than fail the commit.
 *
 * @author Antony Stubbs
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class DeltaListEncodingNotSupportedException extends EncodingNotSupportedException {

    public DeltaListEncodingNotSupportedException(String message) {
        super(message);
    }

    public DeltaListEncodingNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

}
