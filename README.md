# pyspark
ETL utilizando Pyspark, transfiere la data de una tabla mysql a una postgresql

--Tabla MYsql

CREATE TABLE `usuarios` (
  `id` int NOT NULL AUTO_INCREMENT,
  `nombre` varchar(100) NOT NULL,
  `rut_numero` int NOT NULL,
  `rut_dv` char(1) NOT NULL,
  `fecha_nacimiento` date NOT NULL,
  `correo` varchar(255) NOT NULL,
  `telefono` varchar(25) DEFAULT NULL,
  `direccion` text,
  `genero` varchar(10) DEFAULT NULL,
  `estado_civil` varchar(20) DEFAULT NULL,
  `fecha_registro` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=296463 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


--Tabla postgresql


-- public.usuarios definition

-- Drop table

-- DROP TABLE public.usuarios;

CREATE TABLE public.usuarios (
	id int4 NULL,
	nombre text NULL,
	rut_numero int4 NULL,
	rut_dv float8 NULL,
	fecha_nacimiento date NULL,
	correo text NULL,
	telefono text NULL,
	direccion text NULL,
	genero text NULL,
	estado_civil text NULL,
	fecha_registro timestamp NULL
);
