package exception;

public class InputFileException extends RuntimeException{

    public InputFileException(String message) {
        super(String.format("File Path Exception: [%s]", message));
    }
}
