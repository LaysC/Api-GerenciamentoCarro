package org.acme;

import io.quarkus.hibernate.orm.panache.PanacheQuery;
import io.quarkus.panache.common.Sort;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import org.acme.idempotency.Idempotent;

import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Timeout;
import io.smallrye.faulttolerance.api.RateLimit;
import java.time.temporal.ChronoUnit;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Path("/api/v1/fabricantes")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class FabricanteResource {

    @GET
    @Operation(summary = "Retorna todos os fabricantes")
    @APIResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Fabricante.class, type = SchemaType.ARRAY)))
    @RateLimit(value = 10, window = 10, windowUnit = ChronoUnit.SECONDS)
    @Timeout(value = 800, unit = ChronoUnit.MILLIS)
    @CircuitBreaker(requestVolumeThreshold = 5, failureRatio = 0.6, delay = 5000)
    @Fallback(fallbackMethod = "fallbackGetAll")
    public Response getAll(){
        return Response.ok(Fabricante.listAll()).build();
    }

    public Response fallbackGetAll() {
        return Response.ok(Collections.emptyList()).build();
    }

    @GET
    @Path("{id}")
    @Operation(summary = "Retorna um fabricante por ID")
    @APIResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Fabricante.class)))
    @APIResponse(responseCode = "404", description = "Não encontrado")
    @Timeout(value = 500, unit = ChronoUnit.MILLIS)
    @Fallback(fallbackMethod = "fallbackGetById")
    public Response getById(@PathParam("id") long id){
        Fabricante entity = Fabricante.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();
        return Response.ok(entity).build();
    }

    public Response fallbackGetById(long id) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Serviço indisponível.").build();
    }

    @GET
    @Path("/search")
    public Response search(
            @QueryParam("q") String q,
            @QueryParam("sort") @DefaultValue("id") String sort,
            @QueryParam("direction") @DefaultValue("asc") String direction,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("size") @DefaultValue("4") int size
    ){
        Set<String> allowed = Set.of("id", "nome", "detalhes");
        if(!allowed.contains(sort)) sort = "id";

        Sort sortObj = Sort.by(sort, "desc".equalsIgnoreCase(direction) ? Sort.Direction.Descending : Sort.Direction.Ascending);
        PanacheQuery<Fabricante> query;

        if (q == null || q.isBlank()) {
            query = Fabricante.findAll(sortObj);
        } else {
            query = Fabricante.find("lower(nome) like ?1 or lower(detalhes) like ?1", sortObj, "%" + q.toLowerCase() + "%");
        }

        List<Fabricante> fabricantes = query.page(page, size).list();
        var response = new SearchFabricanteResponse();
        response.Fabricantes = fabricantes;
        response.TotalFabricantes = (int) query.count();
        response.TotalPages = query.pageCount();
        response.HasMore = page < query.pageCount() - 1;

        URI nextUri = URI.create("http://localhost:8080/api/v1/fabricantes/search?q=" + (q != null ? q : "") + "&page=" + (page + 1) + "&size=" + size);
        response.NextPage = response.HasMore ? nextUri.toString() : "";

        return Response.ok(response).build();
    }

    @POST
    @Operation(summary = "Cria um fabricante")
    @Parameter(name = "X-Idempotency-Key", in = ParameterIn.HEADER, required = true, description = "Chave de idempotência")
    @RequestBody(content = @Content(schema = @Schema(implementation = Fabricante.class)))
    @APIResponse(responseCode = "201", description = "Criado")
    @Transactional
    @Idempotent
    public Response insert(@Valid Fabricante fabricante){
        Fabricante.persist(fabricante);
        URI location = UriBuilder.fromResource(FabricanteResource.class).path("{id}").build(fabricante.id);
        return Response.created(location).entity(fabricante).build();
    }

    @DELETE
    @Path("{id}")
    @Transactional
    public Response delete(@PathParam("id") long id){
        Fabricante entity = Fabricante.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();

        long musicasVinculadas = Acessorio.count("?1 MEMBER OF fabricantes", entity);
        if(musicasVinculadas > 0){
            return Response.status(Response.Status.CONFLICT)
                    .entity("Não é possível deletar. Existem " + musicasVinculadas + " acessório(s) vinculado(s).")
                    .build();
        }

        Fabricante.deleteById(id);
        return Response.noContent().build();
    }

    @PUT
    @Path("{id}")
    @Transactional
    public Response update(@PathParam("id") long id, @Valid Fabricante newFabricante){
        Fabricante entity = Fabricante.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();
        entity.nome = newFabricante.nome;
        entity.detalhes = newFabricante.detalhes;
        return Response.status(Response.Status.OK).entity(entity).build();
    }
}