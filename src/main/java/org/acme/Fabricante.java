package org.acme;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import java.util.HashSet;
import java.util.Set;

@Entity
public class Fabricante extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Schema(readOnly = true, example = "1")
    public Long id;

    @NotBlank(message = "O nome da fabricante não pode ser vazio")
    @Size(min = 2, max = 50, message = "Nome da fabricante deve ter entre 2 e 50 caracteres")
    public String nome;

    @Size(max = 200, message = "Os detalhes da fabricante não podem ultrapassar 200 caracteres")
    public String detalhes;

    @ManyToMany(mappedBy = "fabricantes", fetch = FetchType.LAZY)
    @JsonIgnore
    public Set<Acessorio> acessorios = new HashSet<>();

    public Fabricante() {}

    public Fabricante(String nome, String detalhes) {
        this.nome = nome;
        this.detalhes = detalhes;
    }
}