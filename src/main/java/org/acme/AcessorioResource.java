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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Path("/api/v1/acessorios")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AcessorioResource {

    @GET
    @Operation(summary = "Retorna todos os acessórios", description = "Retorna uma lista de acessórios.")
    @APIResponse(responseCode = "200", description = "Sucesso", content = @Content(schema = @Schema(implementation = Acessorio.class, type = SchemaType.ARRAY)))
    @RateLimit(value = 10, window = 10, windowUnit = ChronoUnit.SECONDS)
    @Timeout(value = 800, unit = ChronoUnit.MILLIS)
    @CircuitBreaker(requestVolumeThreshold = 5, failureRatio = 0.6, delay = 5000)
    @Fallback(fallbackMethod = "fallbackGetAll")
    public Response getAll(){
        return Response.ok(Acessorio.listAll()).build();
    }

    public Response fallbackGetAll() {
        return Response.ok(Collections.emptyList()).build();
    }

    @GET
    @Path("{id}")
    @Operation(summary = "Retorna um acessório por ID")
    @APIResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Acessorio.class)))
    @APIResponse(responseCode = "404", description = "Não encontrado")
    @Timeout(value = 500, unit = ChronoUnit.MILLIS)
    @Fallback(fallbackMethod = "fallbackGetById")
    public Response getById(@PathParam("id") long id){
        Acessorio entity = Acessorio.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();
        return Response.ok(entity).build();
    }

    public Response fallbackGetById(long id) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Serviço indisponível.").build();
    }

    @GET
    @Operation(summary = "Pesquisa acessórios")
    @APIResponse(responseCode = "200", description = "Sucesso", content = @Content(schema = @Schema(implementation = SearchAcessorioResponse.class)))
    @Path("/search")
    public Response search(
            @QueryParam("q") String q,
            @QueryParam("sort") @DefaultValue("id") String sort,
            @QueryParam("direction") @DefaultValue("asc") String direction,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("size") @DefaultValue("4") int size
    ){
        Set<String> allowed = Set.of("id", "nome", "descricao", "anoAquisicao", "valor", "tempoInstalacaoMinutos");
        if(!allowed.contains(sort)) sort = "id";

        Sort sortObj = Sort.by(sort, "desc".equalsIgnoreCase(direction) ? Sort.Direction.Descending : Sort.Direction.Ascending);
        PanacheQuery<Acessorio> query;

        if (q == null || q.isBlank()) {
            query = Acessorio.findAll(sortObj);
        } else {
            try {
                int numero = Integer.parseInt(q);
                query = Acessorio.find("anoAquisicao = ?1 or tempoInstalacaoMinutos = ?1", sortObj, numero);
            } catch (NumberFormatException e) {
                query = Acessorio.find("lower(nome) like ?1", sortObj, "%" + q.toLowerCase() + "%");
            }
        }

        List<Acessorio> acessorios = query.page(page, size).list();
        var response = new SearchAcessorioResponse();
        response.Acessorios = acessorios;
        response.TotalAcessorios = (int) query.count();
        response.TotalPages = query.pageCount();
        response.HasMore = page < query.pageCount() - 1;

        response.NextPage = response.HasMore ? "http://localhost:8080/api/v1/acessorios/search?q="+(q != null ? q : "")+"&page="+(page + 1) + "&size="+size : "";

        return Response.ok(response).build();
    }

    @POST
    @Operation(summary = "Cria um acessório")
    @Parameter(name = "X-Idempotency-Key", in = ParameterIn.HEADER, required = true, description = "Chave de idempotência")
    @RequestBody(required = true, content = @Content(schema = @Schema(implementation = Acessorio.class)))
    @APIResponse(responseCode = "201", description = "Criado")
    @APIResponse(responseCode = "400", description = "Erro de Validação")
    @Transactional
    @Idempotent
    public Response insert(@Valid Acessorio acessorio){

        if(acessorio.carro != null && acessorio.carro.id != null){
            Carro a = Carro.findById(acessorio.carro.id);
            if(a == null) return Response.status(Response.Status.BAD_REQUEST).entity("Carro não existe").build();
            acessorio.carro = a;
        } else {
            acessorio.carro = null;
        }

        if(acessorio.fabricantes != null && !acessorio.fabricantes.isEmpty()){
            Set<Fabricante> resolved = new HashSet<>();
            for(Fabricante g : acessorio.fabricantes){

                if(g == null || g.id == null || g.id.longValue() == 0) continue;

                Fabricante fetched = Fabricante.findById(g.id);
                if(fetched == null) return Response.status(Response.Status.BAD_REQUEST).entity("Fabricante não existe").build();
                resolved.add(fetched);
            }
            acessorio.fabricantes = resolved;
        } else {
            acessorio.fabricantes = new HashSet<>();
        }

        Acessorio.persist(acessorio);
        URI location = UriBuilder.fromResource(AcessorioResource.class).path("{id}").build(acessorio.id);
        return Response.created(location).entity(acessorio).build();
    }

    @DELETE
    @Path("{id}")
    @Transactional
    public Response delete(@PathParam("id") long id){
        Acessorio entity = Acessorio.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();
        entity.fabricantes.clear();
        entity.persist();
        Acessorio.deleteById(id);
        return Response.noContent().build();
    }

    @PUT
    @Path("{id}")
    @Transactional
    public Response update(@PathParam("id") long id, @Valid Acessorio newAcessorio){
        Acessorio entity = Acessorio.findById(id);
        if(entity == null) return Response.status(Response.Status.NOT_FOUND).build();

        entity.nome = newAcessorio.nome;
        entity.descricao = newAcessorio.descricao;
        entity.anoAquisicao = newAcessorio.anoAquisicao;
        entity.valor = newAcessorio.valor;
        entity.tempoInstalacaoMinutos = newAcessorio.tempoInstalacaoMinutos;

        if(newAcessorio.carro != null && newAcessorio.carro.id != null){
            Carro a = Carro.findById(newAcessorio.carro.id);
            if(a == null) return Response.status(Response.Status.BAD_REQUEST).build();
            entity.carro = a;
        } else {
            entity.carro = null;
        }

        if(newAcessorio.fabricantes != null){
            Set<Fabricante> resolved = new HashSet<>();
            for(Fabricante g : newAcessorio.fabricantes){
                if(g == null || g.id == null || g.id.longValue() == 0) continue;

                Fabricante fetched = Fabricante.findById(g.id);
                if(fetched == null) return Response.status(Response.Status.BAD_REQUEST).build();
                resolved.add(fetched);
            }
            entity.fabricantes = resolved;
        }

        return Response.ok(entity).build();
    }
}