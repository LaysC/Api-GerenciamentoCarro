package org.acme;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import jakarta.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Entity
public class FichaTecnica extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Schema(readOnly = true, example = "1")
    public Long id;

    @Size(max = 2000, message = "Os detalhes do motor não podem ultrapassar 2000 caracteres")
    @Column(length = 2000)
    public String detalhesDoMotor;

    @Size(max = 200, message = "O tipo de combustível não pode ultrapassar 200 caracteres")
    public String tipoDeCombustivel;

    public String opcionaisDeFabrica;

    @OneToOne(mappedBy = "fichaTecnica", fetch = FetchType.LAZY)
    @JsonIgnore
    public Carro carro;

    public FichaTecnica() {}

    public FichaTecnica(String detalhesDoMotor, String tipoDeCombustivel, String opcionaisDeFabrica) {
        this.detalhesDoMotor = detalhesDoMotor;
        this.tipoDeCombustivel = tipoDeCombustivel;
        this.opcionaisDeFabrica = opcionaisDeFabrica;
    }
}