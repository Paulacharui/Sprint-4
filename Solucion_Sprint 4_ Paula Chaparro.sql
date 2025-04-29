-- -----------------------------------------------------Nivell 1--------------------------------------------------------------------
-- Descàrrega els arxius CSV, estudia'ls i dissenya una base de dades amb un esquema d'estrella que contingui, 
-- almenys 4 taules de les quals puguis realitzar les següents consultes:

-- Primer pas: Creació de la base de dades

Create schema sprint_4
;

-- Segon pas creació de les taules

-- Taula Users

create table users (
  id int not null,
  name varchar(100) null,
  surname varchar(100) null,
  phone varchar(150) null,
  personal_email varchar(150) null,
  birth_date varchar(100) null,
  country varchar(150) null,
  city varchar(150) null,
  postal_code varchar(100) null,
  address varchar(255) null,
  primary key (id)
  )
  ;

-- taula credit_cards
create table credit_cards (
id varchar(15) not null,
user_id int default null,
iban varchar(45) default null,
pan varchar(30) default null,
pin varchar(4) default null,
cvv int default null,
track1 varchar(255) default null ,
track2 varchar(255) default null,
expiring_date varchar(20) default null,
primary key (id)
)
;

-- taula companies
create table companies (
company_id varchar(20) not null,
company_name varchar(255) default null,
phone varchar(20) default null,
email varchar(100) default null,
country varchar(100) default null,
website varchar(255) default null,
primary key (company_id)
)
;

-- Taula transactions
create table transactions (
id varchar(255) not null,
card_id  varchar(15) default null,
business_id varchar(20) default null,
timestamp timestamp default null,
amount decimal(10,2) default null,
declined tinyint(1) default null,
product_id varchar(15) default null,
user_id int default null,
lat float default null,
longitude float default null,
primary key (id),
constraint FK_card_id foreign key (card_id) references credit_cards(id),
constraint FK_business_id foreign key (business_id) references companies(company_id),
constraint FK_user_id foreign key (user_id) references users(id)
)
;

--  càrrega de dades des de cvs

set global local_infile = TRUE; -- con esto habilito la opción de poder subir local file
SHOW VARIABLES LIKE 'local_infile'
;

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/users_usa.csv" -- tengo que cambiar la dirección que tienen las barras en origen al copiar por defecto
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows  -- > ignora la línea de encabezado
;

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/users_uk.csv" 
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows  -- > ignora la línea de encabezado
;

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/users_ca.csv" 
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows  -- > ignora la línea de encabezado
;

-- modifico el tipo de dato de declined para que no me salga el warning
alter table transactions
modify declined tinyint default null
; 

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/transactions.csv" 
into table transactions
fields terminated by ';'
enclosed by '"'
lines terminated by '\n'
ignore 1 rows  
;


load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/credit_cards.csv" 
into table credit_cards
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows  
;

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/companies.csv" 
into table companies
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows  
;

-- 1) Realitza una subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules. 

select T.user_id, 
U.name, 
U.surname, 
count(T.id) as counter
from transactions T
left join users U on U.id = T.user_id
group by T.user_id
having counter > 30
;


-- 2) Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules.

select CO.company_name,
CO.company_id,
T.card_id,
C.iban,
round(avg(T.amount),2) as importe_promedio
from transactions T
left join credit_cards C
on T.card_id = C.id
left join companies CO
on T.business_id = CO.company_id
where company_name like 'Donec Ltd'
group by CO.company_name, C.iban, CO.company_id, T.card_id
;

-- -----------------------------------------------------------------Nivell 2-----------------------------------------------------------------

-- Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes tres transaccions van ser declinades 
-- i genera la següent consulta:
-- 1) Quantes targetes estan actives? -->  totes targetes estan actives i en total són 275.


-- Para poder crear la tabla, primero debo identificar las tres últimas transacciones de cada tarjeta y luego verificar si alguna operación está declinada o no.

-- a.)  Calculo las transacciones más recientes de cada tarjeta:

select card_id,
timestamp,
row_number() over(partition by card_id order by date(timestamp)desc) as row_num 
from transactions
order by card_id, timestamp desc
;


-- b.) Agrupo por tarjeta y les asigno un estado en función de si tienen 3 operaciones declinadas. En este punto no tengo en cuenta la fecha. 
-- La consulta indica que todas las tarjetas siguen activas

select card_id,
case 
	when sum(case when declined = 1 then 'declinada' else 0 end) = 3 then 'deactivated'
    else 'active'
    end as card_status
from transactions
group by card_id
;

-- compruebo cuántas tarjetas hay en la tabla transactions (hay 275 tarjetas)

select distinct card_id
from transactions
;

--  voy a ver en general cuántas transacciones hay declinadas: hay 87 transacciones declinadas

select count(*),
card_id
from transactions
where declined = 1
group by card_id
;

-- reviso si  hay alguna tarjeta que tenga varias operaciones declinadas. La consulta indica que no. 

select (card_id),
count(*) as veces_repe
from transactions
where declined = 1 
group by card_id
having count(*) > 0
;

-- c.) Ahora junto todo y teniendo en cuenta solo las últimas 3 transacciones, agrupamos por tarjeta y verificamos si hay alguna que tenga las 3 operaciones
-- más recientes declinadas y creo la tabla:

Create table card_status (
select card_id,
case 
	when sum(case when declined = 1 then 1 else 0 end) = 3 then 'deactivated'
    else 'active'
    end as card_status
from (select card_id,
	timestamp,
    declined,
	row_number() over(partition by card_id order by date(timestamp)desc) as row_num 
	from transactions
	order by card_id, timestamp desc) subquery
where row_num <= 3
group by card_id
)
;

-- d) Creo la relación con la tabla credit_card

alter table card_status 
add primary key(card_id,card_status),
add foreign key(card_id) references credit_cards(id)
;

-- ------------------------------------------------------------------Nivell 3-----------------------------------------------------------------

-- Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada, 
-- tenint en compte que des de transaction tens product_ids. Genera la següent consulta:

-- Creació taula products
create table products (
id varchar(15) not null,
product_name varchar(255) not null,
price varchar(20) default null,
colour varchar(20) default null,
weight float default null,
warehouse_id varchar(15) default null,
primary key (id)
)
;
-- com la taula transactions té una relació N a N amb la taula products, hem de crear una taula intermitja 

CREATE TABLE products_transact (
    transactions_id varchar(255),
    products_id varchar(255),
    PRIMARY KEY (products_id, transactions_id),
    FOREIGN KEY (products_id) REFERENCES products(id),
    FOREIGN KEY (transactions_id) REFERENCES transactions(id)
    )
;
set global local_infile = TRUE;

load data local infile "C:/Users/pchap/Desktop/2. Reskilling Análisis de datos/3. Sprint 4/products.csv" 
into table products
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows  
;

-- 1) Necessitem conèixer el nombre de vegades que s'ha venut cada producte.

-- Para resolver este ejercicio uso like teniendo en cuenta los diferentes formatos en que aparece el código en el csv:
-- Nota: si los datos de origen tuvieran N formatos diferentes, lo que se debería hacer es un paso previo que me armonice el formato (una herramienta de ETL)
-- investigué también la función REGEXP_SUBSTR pero mi versión de Mysql aunque está actualizada no la tiene incluida.

select P.id,
P.product_name,
count(T.product_id) as counter
from products P 
left join transactions T  
on T.product_id like P.id  -- aquí me busca que el producto esté solo
or T.product_id like concat('%, ',P.id, ',%') -- aquí me busca que esté entre otros números
or T.product_id like concat(P.id, ',%') -- este busca que el producto esté en la primera posición de varias
or T.product_id like concat('%, ',P.id) -- este busca la última posición de varios números
where declined = 0 -- > solo contemplo las ventas realizadas y pagadas.
group by P.id
;

-- compruebo el product_id 11 que en la consulta anterior se repitió 40 veces:
select id
from transactions
where ( product_id like '11' 
or product_id like concat('%, ','11,%') 
or product_id like concat('11', ',%') 
or product_id like concat('%, ','11') 
)
and declined = 0
group by id -- efectivamente esta consulta indica que hay 40 registros del producto 11
;

