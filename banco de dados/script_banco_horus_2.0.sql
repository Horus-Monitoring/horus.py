CREATE DATABASE IF NOT EXISTS horus_db;
USE horus_db;

CREATE TABLE empresa (
    id_empresa INT PRIMARY KEY AUTO_INCREMENT,
    razao_social VARCHAR(100) NOT NULL,
    cnpj CHAR(14) NOT NULL UNIQUE,
    telefone_empresa VARCHAR(15)
);

CREATE TABLE servidor (
    id_servidor INT PRIMARY KEY AUTO_INCREMENT,
    hostname VARCHAR(100) NOT NULL UNIQUE,
    endereco_ip VARCHAR(45),
    localizacao VARCHAR(100),
    sistema_operacional VARCHAR(50),
    status_servidor ENUM('Online', 'Offline', 'Atenção', 'Critico') DEFAULT 'Online',
    data_instalacao DATETIME DEFAULT CURRENT_TIMESTAMP,
    fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES empresa(id_empresa)
);

CREATE TABLE funcionario (
    id_funcionario INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    cpf CHAR(11) UNIQUE,
    senha VARCHAR(100),
    funcao ENUM('Analista', 'Gestor'),
    fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES empresa(id_empresa),
    data_cadastro DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE acesso_servidor(
	fk_funcionario INT,
	fk_servidor INT,
	FOREIGN KEY (fk_funcionario) REFERENCES funcionario(id_funcionario),
	FOREIGN KEY (fk_servidor) REFERENCES servidor(id_servidor),
	data_concessao DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fk_funcionario, fk_servidor)
);

CREATE TABLE componente (
    id_componente INT PRIMARY KEY AUTO_INCREMENT,
    tipo VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO componente (tipo) VALUES
('CPU'),
('RAM'),
('DISCO'),
('REDE_RX'),
('REDE_TX'),
('PROCESSOS');

CREATE TABLE servidor_componente (
    fk_servidor INT,
    fk_componente INT,
    unidade_medida VARCHAR(45),
    limite DECIMAL(10,2),
    FOREIGN KEY (fk_servidor) REFERENCES servidor(id_servidor),
    FOREIGN KEY (fk_componente) REFERENCES componente(id_componente),
    PRIMARY KEY (fk_servidor, fk_componente)
);

CREATE TABLE registro_alerta(
	id_registro_alerta INT PRIMARY KEY AUTO_INCREMENT,
    data_alerta DATETIME DEFAULT CURRENT_TIMESTAMP,
    criticidade ENUM("Baixa", "Media", "Alta", "Critica"),
    status_alerta ENUM("Ativo", "Resolvido"),
    fk_servidor INT,
    fk_componente INT,
    FOREIGN KEY (fk_servidor, fk_componente)
		REFERENCES servidor_componente(fk_servidor, fk_componente)
);

CREATE TABLE contato (
    id_contato INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(100),
    email VARCHAR(100),
    titulo_mensagem VARCHAR(100),
    mensagem VARCHAR(255)
);

INSERT INTO empresa (razao_social, cnpj, telefone_empresa)
VALUES ('Empresa Monitoramento', '12345678000199', '11999999999');

INSERT INTO servidor (
    hostname,
    endereco_ip,
    localizacao,
    sistema_operacional,
    fk_empresa
)
VALUES (
    'Nathan',
    '192.168.0.10',
    'São Paulo',
    'Windows/Linux',
    1
);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'CPU'), 10);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'RAM'), 10);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'DISCO'), 10);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'REDE_RX'), 0);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'REDE_TX'), 0);

INSERT INTO servidor_componente (fk_servidor, fk_componente, limite)
VALUES (1, (SELECT id_componente FROM componente WHERE tipo = 'PROCESSOS'), 0);