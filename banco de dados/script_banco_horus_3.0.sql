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
    mac_address CHAR(17) NOT NULL UNIQUE,
    hostname VARCHAR(100) NOT NULL UNIQUE,
    endereco_ip VARCHAR(45),
    localizacao VARCHAR(100),
    sistema_operacional VARCHAR(50),
    status_servidor ENUM('Online', 'Offline', 'Atenção', 'Crítico') DEFAULT 'Online',
    data_instalacao DATETIME DEFAULT CURRENT_TIMESTAMP,
	data_status DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
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
    chave VARCHAR(45) NOT NULL UNIQUE,
    titulo VARCHAR(100) NOT NULL,
    status_alerta ENUM("Ativo", "Resolvido"),
    criticidade ENUM("Baixo", "Medio", "Alto", "Crítico"),
    data_alerta DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    data_resolucao DATETIME DEFAULT CURRENT_TIMESTAMP,    
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
VALUES ('Horus Tech', '12345678000199', '11999999999');

INSERT INTO componente (tipo) VALUES
('CPU'),
('RAM'),
('DISCO'),
('LATENCIA'),
('PROCESSOS');

insert into funcionario VALUES 
	(DEFAULT, 
    'João Ricardo', 
    'ricardo@horus.com', 
    '45290856862', 
    'Abcdef@123456789', 
    'Analista', 
    1, 
    localtimestamp()),
    (DEFAULT,
    'Letícia Costa',
    'leticia@horus.com',
    '11122233344',
    'Abcdef@123456789',
    'Gestor',
    1,
    localtimestamp()),
    (DEFAULT,
    'Vitória Lima',
    'vitoria@horus.com',
    '44433322211',
    'Abcdef@12345789',
    'Gestor',
    1,
    localtimestamp()),
    (DEFAULT,
    'Nathan Morais',
    'nathan@horus.com',
    '11122244433',
    'Abcdef@123456789',
    'Gestor',
    1,
    localtimestamp()),
    (DEFAULT,
    'Gustavo Pietro',
    'gustavo@horus.com',
    '22211144433',
    'Abcdef@123456789',
    'Analista',
    1,
    localtimestamp()),
    (DEFAULT,
    'Matheus Barros',
    'matheus@horus.com',
    '44433311122',
    'Abcdef@123456789',
    'Analista',
    1,
    localtimestamp());
    
SELECT id_servidor, fk_empresa, mac_address
        FROM servidor
        WHERE mac_address = "80:30:49:0c:87:29";

SELECT * FROM servidor;

INSERT INTO servidor (
    hostname,
    mac_address,
    endereco_ip,
    localizacao,
    sistema_operacional,
    status_servidor,
    data_status,
    fk_empresa
)
VALUES
    ('NeithaNitro',
    '80:30:49:0c:87:29',
    '192.168.0.10',
    'Data Center SP',
    'Linux/Windows',
    'Online',
    localtimestamp(),
    1),
    ('Jortieke',
    '192.168.56.1',
    'c0:35:32:c7:0b:59',
    'Data Center RJ',
    'Windows 11',
    'Crítico',
    localtimestamp(),    
    1),
    ('Vivian',
    '0A:00:27:00:00:11',
    '192.168.43.1',
    'Data Center SP',
     'Ubuntu',
    'Offline',
    localtimestamp(),
    1),
    ('DESKTOP-N99CUBI',
    '10:FF:E0:0F:99:42',
    '192.168.32.1',
    'Data Center SP',
    'Windows XP',
    'Online',
    localtimestamp(),
    1);
    
INSERT INTO acesso_servidor VALUES
	(1,2, DEFAULT),
    (1,1, DEFAULT),
    (2,1, DEFAULT),
    (2,2, DEFAULT),
    (2,3, DEFAULT),
    (2,4, DEFAULT),
    (3,1, DEFAULT),
    (3,2, DEFAULT),
    (3,3, DEFAULT),
    (3,4, DEFAULT),
    (4,1, DEFAULT),
    (4,2, DEFAULT),
    (4,3, DEFAULT),
    (4,4, DEFAULT),
    (5,4, DEFAULT),
    (5,2, DEFAULT),
    (6,2, DEFAULT),
    (6,3, DEFAULT);

INSERT INTO servidor_componente VALUES (1, 1, '%', 30), (2, 1, '%', 70), (3, 1, '%', 50), (4, 1, '%', 90);

INSERT INTO servidor_componente VALUES (1, 2, '%', 25), (2, 2, '%', 45), (3, 2, '%', 65), (4, 2, '%', 95);

INSERT INTO servidor_componente VALUES (1, 3, '%', 20), (2, 3, '%', 70), (3, 3, '%', 20), (4, 3, '%', 80);

INSERT INTO servidor_componente VALUES (1, 4, 's', 10), (2, 4, 's', 10), (3, 4, 's', 5), (4, 4, 's', 3);

INSERT INTO servidor_componente VALUES (1, 5, 'qtd', 0), (4, 5, 'qtd', 0);