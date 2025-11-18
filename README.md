# code-with-quarkus

Este projeto utiliza o **Quarkus**, o *Supersonic Subatomic Java Framework*.

Se você deseja saber mais sobre o Quarkus, acesse o site oficial:  
[https://quarkus.io/](https://quarkus.io/)

---

## Executando a aplicação em modo de desenvolvimento

Você pode executar a aplicação em modo de desenvolvimento, o que permite **live coding**, utilizando o comando:

`./mvnw quarkus:dev`

**Nota**: O Quarkus agora conta com uma **Dev UI**, acessível apenas no modo de desenvolvimento, no endereço:  
`http://localhost:8080/q/dev/`

---

## Empacotando e executando a aplicação

A aplicação pode ser empacotada com o comando:

`./mvnw package`

Esse processo irá gerar o arquivo `quarkus-run.jar` dentro do diretório `target/quarkus-app/`.  
Note que esse **não é um über-jar**, pois as dependências são copiadas separadamente para o diretório `target/quarkus-app/lib/`.

Você pode executar a aplicação empacotada com:

`java -jar target/quarkus-app/quarkus-run.jar`

### Criando um Über-Jar

Se desejar criar um über-jar (jar completo com todas as dependências), execute o comando:

`./mvnw package -Dquarkus.package.jar.type=uber-jar`

O über-jar será gerado e poderá ser executado com:

`java -jar target/*-runner.jar`

---

## Criando um executável nativo

Você pode criar um executável nativo utilizando:

`./mvnw package -Dnative`

Se você **não tiver o GraalVM instalado**, é possível realizar a build nativa dentro de um container:

`./mvnw package -Dnative -Dquarkus.native.container-build=true`

Após a build, você poderá executar o binário gerado diretamente:

`./target/code-with-quarkus-1.0.0-SNAPSHOT-runner`

Para mais informações sobre como construir executáveis nativos, acesse:  
[https://quarkus.io/guides/maven-tooling](https://quarkus.io/guides/maven-tooling)

---

## Guias Relacionados

- **REST (guide)**: Implementação de Jakarta REST utilizando processamento em tempo de build e Vert.x.  
  > ⚠️ Esta extensão não é compatível com `quarkus-resteasy` ou extensões que dependem dela.

- **JDBC Driver - H2 (guide)**: Conecte-se ao banco de dados H2 via JDBC.

- **REST Jackson (guide)**: Suporte à serialização com Jackson no Quarkus REST.  
  > ⚠️ Esta extensão também não é compatível com `quarkus-resteasy`.

- **Hibernate ORM with Panache (guide)**: Simplifica o uso do Hibernate ORM com os padrões *Active Record* ou *Repository*.

---

## Código Fornecido

### Hibernate ORM

- Crie sua primeira entidade JPA  
- Consulte a seção relacionada no guia oficial do Quarkus

### REST

- Inicie facilmente seus serviços Web REST  
- Consulte a seção relacionada no guia oficial do Quarkus

---
