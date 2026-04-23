CREATE DATABASE horus_db;
USE horus_db;

CREATE TABLE Contato_inicial (
    idContato_inicial INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(45),
    sobrenome VARCHAR(45),
    email VARCHAR(45),
    mensagem VARCHAR(255)
); 

CREATE TABLE Localizacao (
    idLocalizacao INT PRIMARY KEY AUTO_INCREMENT,
    uf CHAR(2),
    cidade VARCHAR(45),
    bairro VARCHAR(45),
    logradouro VARCHAR(45),
    numero INT,
    cep CHAR(11)
);

CREATE TABLE Empresa (
    idEmpresa INT PRIMARY KEY AUTO_INCREMENT,
    razao_social VARCHAR(45),
    cnpj CHAR(15) NOT NULL UNIQUE,
    telefone_empresa CHAR(11) NOT NULL UNIQUE,
    token_empresa CHAR(8) NOT NULL UNIQUE,
    fk_localizacao INT UNIQUE,
    FOREIGN KEY (fk_localizacao)
        REFERENCES Localizacao(idLocalizacao)
);

CREATE TABLE Papel (
    idPapel INT PRIMARY KEY AUTO_INCREMENT,
    nivel VARCHAR(45),
    descricao VARCHAR(80),
    fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES Empresa(idEmpresa)
);

CREATE TABLE Funcionario (
    idFuncionario INT PRIMARY KEY AUTO_INCREMENT,
    nome VARCHAR(45),
    nome_social VARCHAR(45),
    cpf CHAR(11) NOT NULL UNIQUE,
    email VARCHAR(45) NOT NULL UNIQUE,
    senha VARCHAR(45),
    fk_papel INT,
    imagem VARCHAR(255),
    FOREIGN KEY (fk_papel) REFERENCES Papel(idPapel)
);

CREATE TABLE Servidor (
    idServidor INT PRIMARY KEY AUTO_INCREMENT,
    data_instalacao DATE,
    tag_servidor VARCHAR(45),
    fk_empresa INT,
    FOREIGN KEY (fk_empresa) REFERENCES Empresa(idEmpresa)
);

CREATE TABLE Componentes (
    idComponentes INT PRIMARY KEY AUTO_INCREMENT,
    nome_componente VARCHAR(45),
    tipo_componente VARCHAR(45),
    unidade_medida VARCHAR(45)
);

CREATE TABLE CompServidor (
    id_componente_v INT PRIMARY KEY AUTO_INCREMENT,
    fk_componente INT,
    fk_servidor INT,
    limite DECIMAL(10,2),
    ativo BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (fk_componente) REFERENCES Componentes(idComponentes),
    FOREIGN KEY (fk_servidor) REFERENCES Servidor(idServidor)
);

CREATE TABLE Registro_Alerta (
    idAlerta INT PRIMARY KEY AUTO_INCREMENT,
    data_alerta DATETIME DEFAULT CURRENT_TIMESTAMP,
    criticidade VARCHAR(45),
    valor DECIMAL(10,2),
    fk_servidor_componentes INT,
    FOREIGN KEY (fk_servidor_componentes)
        REFERENCES CompServidor(id_componente_v)
);

INSERT INTO Localizacao (uf, cidade, bairro, logradouro, numero, cep)
VALUES ('SP', 'São Paulo2', 'Centro2', 'Rua A2', 109, '01000001');

INSERT INTO Empresa (razao_social, cnpj, telefone_empresa, token_empresa, fk_localizacao)
VALUES ('Empresa Teste2', '12345678000198', '11999999998', 'ABC12344', 2);

INSERT INTO Componentes (nome_componente, tipo_componente, unidade_medida) VALUES
('CPU', 'CPU', '%'),
('Memoria', 'RAM', '%'),
('Disco', 'DISCO', '%'),
('Rede', 'REDE', 'Mbps');

INSERT INTO Servidor (tag_servidor, fk_empresa) VALUES
('Servirdor 2', '2');


INSERT INTO CompServidor (fk_componente, fk_servidor, limite, ativo) VALUES
(1, 2, 10, TRUE),
(2, 2, 10, TRUE),
(3, 2, 10, TRUE),
(4, 2, 10, TRUE);

select * from Registro_Alerta;
select * from Servidor;