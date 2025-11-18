package org.acme;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import java.util.HashSet;
import java.util.Set;

@Entity
public class Acessorio extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Schema(readOnly = true, example = "1")
    public Long id;

    @NotBlank(message = "O nome do acessório não pode ser vazio")
    @Size(min = 1, max = 200)
    public String nome;

    @NotBlank(message = "A descrição é obrigatória")
    @Size(max = 2000)
    public String descricao;

    @Min(value = 1900, message = "Ano de aquisição inválido")
    public int anoAquisicao;

    @DecimalMin(value = "0.0", inclusive = true, message = "Valor mínimo é 0.0")
    @DecimalMax(value = "100000.0", inclusive = true, message = "Valor máximo é 100000.0")
    public double valor;

    @Min(value = 0, message = "Tempo de instalação não pode ser negativo")
    public int tempoInstalacaoMinutos;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "carro_id")
    public Carro carro;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "acessorio_fabricante",
            joinColumns = @JoinColumn(name = "acessorio_id"),
            inverseJoinColumns = @JoinColumn(name = "fabricante_id")
    )
    public Set<Fabricante> fabricantes = new HashSet<>();

    public Acessorio() {}

    public Acessorio(Long id, String nome, String descricao, int anoAquisicao, double valor, int tempoInstalacaoMinutos) {
        this.id = id;
        this.nome = nome;
        this.descricao = descricao;
        this.anoAquisicao = anoAquisicao;
        this.valor = valor;
        this.tempoInstalacaoMinutos = tempoInstalacaoMinutos;
    }
}