-- This file allow to write SQL commands that will be emitted in test and dev.
-- The commands are commented as their support depends of the database

-- 1. Insere dados na tabela FichaTecnica (Antigo PerfilArtista)
insert into FichaTecnica (detalhesDoMotor, tipoDeCombustivel, opcionaisDeFabrica) values(
                                                                                            'Motor V8 5.0L de alta performance, com injeção direta. Potência de 450 cavalos e torque de 56 kgfm. Câmbio automático de 10 velocidades.',
                                                                                            'Gasolina de Alta Octanagem',
                                                                                            'Sistema de som Premium Bang & Olufsen, Pacote de Segurança Ativa Completo (Frenagem de Emergência, Piloto Adaptativo).'
                                                                                        );

insert into FichaTecnica (detalhesDoMotor, tipoDeCombustivel, opcionaisDeFabrica) values(
                                                                                            'Motor 2.0L turbo, 4 cilindros em linha. Desenvolvido para balancear performance e economia. Potência de 220 cavalos e tração integral.',
                                                                                            'Flex (Etanol/Gasolina)',
                                                                                            'Teto solar panorâmico, Acabamento interno em fibra de carbono, Rodas esportivas aro 19.'
                                                                                        );

insert into FichaTecnica (detalhesDoMotor, tipoDeCombustivel, opcionaisDeFabrica) values(
                                                                                            'Motor elétrico de 300kW com bateria de 80 kWh. Alcance de 450 km por carga. Aceleração instantânea e zero emissão.',
                                                                                            'Elétrico',
                                                                                            'Piloto Automático Avançado (Full Self-Driving), Interior vegano, Carregador wireless para celular.'
                                                                                        );

insert into FichaTecnica (detalhesDoMotor, tipoDeCombustivel, opcionaisDeFabrica) values(
                                                                                            'Motor 1.4L TSI, focado em economia e durabilidade. 4 cilindros. Potência de 150 cavalos.',
                                                                                            'Gasolina',
                                                                                            'Kit multimídia de 8 polegadas, Sensor de estacionamento traseiro, Airbags laterais.'
                                                                                        );

insert into FichaTecnica (detalhesDoMotor, tipoDeCombustivel, opcionaisDeFabrica) values(
                                                                                            'Motor V6 3.5L aspirado. Conforto e silêncio como prioridade. 280 cavalos. Transmissão CVT.',
                                                                                            'Gasolina',
                                                                                            'Bancos de couro com aquecimento, Câmera de ré 360°, Faróis de LED adaptativos.'
                                                                                        );

-- 2. Insere dados na tabela Carro (Antigo Artista)
insert into Carro (modelo, nomeCompletoVersao, dataDeFabricacao, paisDeMontagem, ficha_tecnica_id) values('Mustang GT', 'Ford Mustang GT Premium', '2023-01-01', 'Estados Unidos', 1);
insert into Carro (modelo, nomeCompletoVersao, dataDeFabricacao, paisDeMontagem, ficha_tecnica_id) values('Golf R', 'Volkswagen Golf R Performance', '2022-06-27', 'Alemanha', 2);
insert into Carro (modelo, nomeCompletoVersao, dataDeFabricacao, paisDeMontagem, ficha_tecnica_id) values('Model 3', 'Tesla Model 3 Long Range', '2021-09-01', 'Estados Unidos', 3);
insert into Carro (modelo, nomeCompletoVersao, dataDeFabricacao, paisDeMontagem, ficha_tecnica_id) values('Jetta', 'Volkswagen Jetta Comfortline', '2023-05-18', 'México', 4);
insert into Carro (modelo, nomeCompletoVersao, dataDeFabricacao, paisDeMontagem, ficha_tecnica_id) values('Fusion', 'Ford Fusion Titanium', '2019-01-01', 'México', 5);

-- 3. Insere dados na tabela Fabricante (Antigo GeneroMusical)
insert into Fabricante (nome, detalhes) values('BOSCH', 'Líder em sistemas de freios e injeção eletrônica de combustível.');
insert into Fabricante (nome, detalhes) values('Michelin', 'Fabricante francesa de pneus de alta durabilidade e performance.');
insert into Fabricante (nome, detalhes) values('Recaro', 'Marca alemã especializada em bancos esportivos e de alta ergonomia.');
insert into Fabricante (nome, detalhes) values('Alpine', 'Fabricante de sistemas de áudio e multimídia de alta qualidade para veículos.');
insert into Fabricante (nome, detalhes) values('Garrett', 'Especializada em turbocompressores para motores de combustão interna.');
insert into Fabricante (nome, detalhes) values('Brembo', 'Fabricante italiana de sistemas de freios de alto desempenho.');

-- 4. Insere dados na tabela Acessorio (Antigo Musica)
insert into Acessorio (nome, descricao, anoAquisicao, valor, tempoInstalacaoMinutos, carro_id) values(
                                                                                                         'Spoiler Traseiro',
                                                                                                         'Spoiler aerodinâmico de fibra de carbono para maior estabilidade em altas velocidades.',
                                                                                                         2023, 1500.00, 45, 2
                                                                                                     );

insert into Acessorio (nome, descricao, anoAquisicao, valor, tempoInstalacaoMinutos, carro_id) values(
                                                                                                         'Filtro de Ar Esportivo',
                                                                                                         'Filtro de ar de alta vazão reutilizável. Melhora a performance do motor.',
                                                                                                         2024, 350.00, 15, 2
                                                                                                     );

insert into Acessorio (nome, descricao, anoAquisicao, valor, tempoInstalacaoMinutos, carro_id) values(
                                                                                                         'Kit de Faróis LED',
                                                                                                         'Faróis de LED de última geração com sistema de luz diurna (DRL).',
                                                                                                         2023, 1200.00, 120, 3
                                                                                                     );

insert into Acessorio (nome, descricao, anoAquisicao, valor, tempoInstalacaoMinutos, carro_id) values(
                                                                                                         'Tapetes Personalizados',
                                                                                                         'Conjunto de tapetes internos de borracha de alta resistência, personalizados com o logo.',
                                                                                                         2024, 210.00, 5, 4
                                                                                                     );

insert into Acessorio (nome, descricao, anoAquisicao, valor, tempoInstalacaoMinutos, carro_id) values(
                                                                                                         'Central Multimídia',
                                                                                                         'Central multimídia de 10 polegadas com GPS integrado e compatibilidade com Android Auto/Apple CarPlay.',
                                                                                                         2023, 1800.00, 260, 5
                                                                                                     );

-- 5. Associações acessorio-fabricante (Many-to-Many)
-- Tabela de junção alterada de 'musica_genero' para 'acessorio_fabricante'
-- Colunas de ID alteradas
insert into acessorio_fabricante (acessorio_id, fabricante_id) values (1, 6), (1, 2);
insert into acessorio_fabricante (acessorio_id, fabricante_id) values (2, 5);
insert into acessorio_fabricante (acessorio_id, fabricante_id) values (3, 1), (3, 2), (3, 3);
insert into acessorio_fabricante (acessorio_id, fabricante_id) values (4, 2);
insert into acessorio_fabricante (acessorio_id, fabricante_id) values (5, 4), (5, 5);