package org.acme.exception;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.ArrayList;
import java.util.List;

@Provider
public class ValidationExceptionMapper implements ExceptionMapper<ConstraintViolationException> {

    @Override
    public Response toResponse(ConstraintViolationException exception) {
        List<String> erros = new ArrayList<>();

        for (ConstraintViolation<?> violation : exception.getConstraintViolations()) {
            String mensagem = violation.getMessage();
            // Tenta pegar o nome do campo (ex: modelo)
            String campo = "";
            try {
                String[] parts = violation.getPropertyPath().toString().split("\\.");
                campo = parts[parts.length - 1] + ": ";
            } catch (Exception e) {
                campo = "";
            }
            erros.add(campo + mensagem);
        }

        return Response.status(400)
                .entity(new ErrorBody(400, "Erro de Validação", erros))
                .build();
    }

    public static class ErrorBody {
        public int status;
        public String message;
        public List<String> errors;

        public ErrorBody(int s, String m, List<String> e) {
            this.status = s;
            this.message = m;
            this.errors = e;
        }
    }
}