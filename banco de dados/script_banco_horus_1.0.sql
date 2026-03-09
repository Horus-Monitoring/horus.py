CREATE DATABASE horus_db;

USE horus_db;


CREATE TABLE Contato_inicial (
idContato_inicial INT PRIMARY KEY AUTO_INCREMENT,
nome VARCHAR(45),
sobrenome VARCHAR(45),
emaill VARCHAR(45),
mensagem VARCHAR(240)
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
fk_localizacao INT,
CONSTRAINT fk_localizacao_registro
	FOREIGN KEY (fk_localizacao)
		REFERENCES Localizacao(idLocalizacao)
);




CREATE TABLE Papel (
idPapel INT  AUTO_INCREMENT,
nivel VARCHAR(45),
descricao VARCHAR(80),
fk_empresa INT, 
CONSTRAINT pk_papel_empresa
	PRIMARY KEY(idPapel, fk_empresa),
CONSTRAINT fk_empresa_registro
	FOREIGN KEY (fk_empresa)
		REFERENCES Empresa (idEMpresa)
);






CREATE TABLE Funcionario (
idFuncionario INT AUTO_INCREMENT,
nome VARCHAR(45),
nome_social VARCHAR(45),
cpf CHAR(11) NOT NULL UNIQUE,
email VARCHAR(45) NOT NULL UNIQUE,
senha VARCHAR(45),
fk_papel_empresa INT,
CONSTRAINT pk_funcionario_papel_empresa
	PRIMARY KEY(idFuncionario, fk_papel_empresa),
CONSTRAINT fk_papel_registro
	FOREIGN KEY (fk_papel_empresa)
		REFERENCES Papel(idPapel),
foto_usuario VARCHAR(255)
);






 
 
 CREATE TABLE Servidor (
idServidor INT AUTO_INCREMENT,
data_instalacao DATE,
tag_servidor VARCHAR(45),
fk_empresa INT,
 CONSTRAINT pk_servidor_empresa
	PRIMARY KEY (idServidor, fk_empresa),
CONSTRAINT fk_empresa_registro_servidor
	FOREIGN KEY (fk_empresa)
		REFERENCES Empresa(idEmpresa)
);

CREATE TABLE Componentes (
idComponentes INT PRIMARY KEY AUTO_INCREMENT,
nome_componente VARCHAR(45),
tipo_componente VARCHAR(45),
unidade_medida VARCHAR(45)
);


CREATE TABLE CompServidor (
id_componente_v INT AUTO_INCREMENT,
fk_componente INT,
fk_servidor INT,
limite VARCHAR(45),
CONSTRAINT pk_componente_servidor_componente_v
	PRIMARY KEY (id_componente_v, fk_componente, fk_servidor),
CONSTRAINT fk_componente_registro
	FOREIGN KEY (fk_componente)
		REFERENCES Componentes(idComponentes),
CONSTRAINT fk_servidor_registro
	FOREIGN KEY (fk_servidor)
		REFERENCES Servidor(idServidor)
);


CREATE TABLE Registro_Alerta (
idAlerta INT PRIMARY KEY AUTO_INCREMENT,
data_alerta DATETIME DEFAULT CURRENT_TIMESTAMP, 
criticidade VARCHAR(45), 
fk_servidor_componentes INT,
CONSTRAINT fk_servidor_componente
	FOREIGN KEY (fk_servidor_componentes)
		REFERENCES CompServidor(id_componente_v)
); 




INSERT INTO Localizacao (uf, cidade, bairro, logradouro, numero, cep) VALUES
	('SP', 'São Paulo', 'Paulista','Rua Hadock Lobo', 12, '08412210');
    
INSERT INTO Empresa (razao_social, cnpj, telefone_empresa, token_empresa, fk_localizacao) VALUES
	('LIPSU AERO', '123456789101234', '5573-1234', 'ABC12345', 1);
    
INSERT INTO Papel (nivel, descricao, fk_empresa) VALUES
	('Gerente de ATC', 'Deve monitorar e solucionar problemas', 1);
    
INSERT INTO Funcionario (fk_papel_empresa, nome,  cpf, email, senha) VALUES
	(1, 'Herycka', '32187634567', 'herycka@gmail.com', 'Herycka_1234');
    
    
SELECT  nivel  FROM Papel JOIN Funcionario
	ON idPapel = fk_papel_empresa
    WHERE nome = 'Herycka';
    