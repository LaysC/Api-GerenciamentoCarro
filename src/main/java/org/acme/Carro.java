package org.acme;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Entity
public class Carro extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Schema(readOnly = true, example = "1")
    public Long id;

    @NotBlank(message = "O modelo do carro não pode ser vazio")
    @Size(min = 2, max = 100, message = "O modelo deve ter entre 2 e 100 caracteres")
    public String modelo;

    public String nomeCompletoVersao;

    @Past(message = "A data de fabricação deve ser no passado")
    public LocalDate dataDeFabricacao;

    @NotBlank(message = "O país de montagem é obrigatório")
    @Size(max = 80)
    public String paisDeMontagem;

    @OneToOne(cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    @JoinColumn(name = "ficha_tecnica_id")
    public FichaTecnica fichaTecnica;

    @OneToMany(mappedBy = "carro", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @JsonIgnore
    public List<Acessorio> acessorios = new ArrayList<>();

    public Carro() {}

    public Carro(Long id, String modelo, String nomeCompletoVersao, LocalDate dataDeFabricacao, String paisDeMontagem, FichaTecnica fichaTecnica) {
        this.id = id;
        this.modelo = modelo;
        this.nomeCompletoVersao = nomeCompletoVersao;
        this.dataDeFabricacao = dataDeFabricacao;
        this.paisDeMontagem = paisDeMontagem;
        this.fichaTecnica = fichaTecnica;
    }
}