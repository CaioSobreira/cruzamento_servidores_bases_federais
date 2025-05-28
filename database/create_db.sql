CREATE SCHEMA benef_federais;
CREATE SCHEMA resultados;


CREATE TABLE resultados.seguro_defeso (
    nome TEXT,
    cpf TEXT,
    pis_pasep TEXT,
    vinculos TEXT,
    remuneracao_bruta TEXT,
    mes_referencia TEXT,
    uf CHAR(2),
    codigo_municipio_siafi TEXT,
    nome_municipio TEXT,
    cpf_favorecido TEXT,
    nis_favorecido TEXT,
    rgp_favorecido TEXT,
    nome_favorecido TEXT,
    valor_parcela TEXT,
    data_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);





CREATE TABLE resultados.bpc (
    nome TEXT,
    cpf TEXT,
    pis_pasep TEXT,
    vinculos TEXT,
    remuneracao_bruta TEXT,
    mes_competencia TEXT,
    mes_referencia TEXT,
    uf TEXT,
    codigo_municipio_siafi TEXT,
    nome_municipio TEXT,
    nis_beneficiario TEXT,
    cpf_beneficiario TEXT,
    nome_beneficiario TEXT,
    nis_representante_legal TEXT,
    cpf_representante_legal TEXT,
    nome_representante_legal TEXT,
    numero_beneficio TEXT,
    beneficio_concedido_judicialmente TEXT,
    valor_parcela TEXT,
    data_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE resultados.novo_bolsa_familia (
    nome TEXT,
    cpf TEXT,
    pis_pasep TEXT,
    vinculos TEXT,
    remuneracao_bruta TEXT,
    mes_competencia TEXT,
    mes_referencia TEXT,
    uf TEXT,
    codigo_municipio_siafi TEXT,
    nome_municipio TEXT,
    cpf_favorecido TEXT,
    nis_favorecido TEXT,
    nome_favorecido TEXT,
    valor_parcela TEXT,
    data_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE benef_federais.bpc (
	mes_competencia varchar(6) NULL,
	mes_referencia varchar(6) NULL,
	uf varchar(2) NULL,
	codigo_municipio_siafi varchar(4) NULL,
	nome_municipio varchar NULL,
	nis_beneficiario varchar(11) NULL,
	cpf_beneficiario varchar(14) NULL,
	nome_beneficiario varchar NULL,
	nis_representante_legal varchar(11) NULL,
	cpf_representante_legal varchar(14) NULL,
	nome_representante_legal varchar NULL,
	numero_beneficio varchar(10) NULL,
	beneficio_concedido_judicialmente varchar(3) NULL,
	valor_parcela varchar NULL
);

CREATE TABLE benef_federais.novo_bolsa_familia (
	mes_competencia varchar(6) NULL,
	mes_referencia varchar(6) NULL,
	uf varchar(2) NULL,
	codigo_municipio_siafi varchar(4) NULL,
	nome_municipio varchar NULL,
	cpf_favorecido varchar(14) NULL,
	nis_favorecido varchar(11) NULL,
	nome_favorecido varchar NULL,
	valor_parcela varchar NULL
);

CREATE TABLE benef_federais.seguro_defeso (
	mes_referencia varchar(6) NULL,
	uf varchar(2) NULL,
	codigo_municipio_siafi varchar(4) NULL,
	nome_municipio varchar NULL,
	cpf_favorecido varchar(14) NULL,
	nis_favorecido varchar(11) NULL,
	rgp_favorecido varchar(10) NULL,
	nome_favorecido varchar NULL,
	valor_parcela varchar NULL
);

CREATE SCHEMA servidores;

CREATE TABLE servidores.servidores_cruzamento (
	nome varchar NULL,
	cpf varchar(11) NULL,
	pis_pasep varchar(11) NULL,
	vinculos varchar NULL,
	remuneracao_bruta varchar NULL
);

