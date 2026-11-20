package lakewire.admin.api;

import lakewire.coordinator.CoordinatorException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class AdminExceptionHandler {

    @ExceptionHandler(CoordinatorException.class)
    ResponseEntity<ErrorBody> coordinator(CoordinatorException failure) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ErrorBody("coordinator_unavailable", failure.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ErrorBody> badRequest(IllegalArgumentException failure) {
        return ResponseEntity.badRequest().body(new ErrorBody("invalid_request", failure.getMessage()));
    }
}
