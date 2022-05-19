select name, count(*) from user_property group by  name


select * from user_property limit 100

select * from user_company join (
    select *
    from crosstab('select user_id,name, value from  user_property where user_id in ((select id from user_company  where identification is not null limit 500) union (select id from user_company  where identification is not null limit 500))')
             as ct(user_id integer, "Acepto compartir mis datos de contacto " varchar,
                   "Acepto políticas de privacidad, los Términos y condiciones"
                       varchar, "Acepto términos y condiciones"
                       varchar, "Apellidos"
                       varchar, "Cédula"
                       varchar, "Ciudad"
                       varchar, "Código postal"
                       varchar, "Complemento dirección"
                       varchar, "Correo electrónico"
                       varchar, "ctl00__cphPrincipal_chkTerminosCondiciones"
                       varchar, "ctl00__cphPrincipal_cmbEstrato"
                       varchar, "ctl00__cphPrincipal_ddlHijos"
                       varchar, "ctl00__cphPrincipal_ddlTipoDocumento"
                       varchar, "ctl00__cphPrincipal_txtDirEstandarizada"
                       varchar, "ctl00__cphPrincipal_txtInicioSuscripcion"
                       varchar, "ctl00__cphPrincipal_txtNit"
                       varchar, "ctl00__cphPrincipal_txtSegundoApellido"
                       varchar, "ctl00__cphPrincipal_txtSegundoNombre"
                       varchar, "ctl00__cphPrincipal_txtTelefono"
                       varchar, "Dirección"
                       varchar, "Direccion2"
                       varchar, "ElColombianoLogin"
                       varchar, "Especificaciones"
                       varchar, "Fecha suscripción"
                       varchar, "GenteVecinos"
                       varchar, "hddDireccion"
                       varchar, "hddFechaNegocioMin"
                       varchar, "Identificacion"
                       varchar, "mobile_number"
                       varchar, "Naturaleza"
                       varchar, "Nick name"
                       varchar, "Nombre"
                       varchar, "Nombre de la empresa"
                       varchar, "Nro_Hijos"
                       varchar, "Número documento"
                       varchar, "Origen"
                       varchar, "PropiedadesSolicitudProyecto"
                       varchar, "QhuboSuscribete"
                       varchar, "T1"
                       varchar, "T2"
                       varchar, "T3"
                       varchar, "T4"
                       varchar, "Telefono"
                       varchar, "Telefono2"
                       varchar, "Tipo_Identificacion"
                       varchar, "Usuario"
                       varchar, "VecinosEmprendedores"
                       varchar, "VecinosGen"
                       varchar, "Zona" varchar)
)  as prop on user_company.id=prop.user_id


query_crostab = f"""select * from
            (select * from crosstab('select event_id, name, value from event_property where name in
            (select name from user_event where user_event.id ='{event_id}' limit 5000)') as ct())"""










select
  *
from
  crosstab(
	'select event_id, name, value from event_property where event_id in (select id from user_event where name like ''SGC_SPEC'')'
  ) as ct(
	event_id integer, "Numero de linea" text, "Periodo" text,
	"Codigo de Periodo" text, "FECHA_TERMINO" date,
	"FECHA_INICIO" date, "Valor IVA" integer,
	"Valor Total" integer, "Precio Neto" integer,
	"Precio" integer, "Cantidad Cortesía" integer,
	"Cantidad Ordenada" integer, "Código Lista de Precio" text,
	"Procedencia Pedido" text, "Código frecuencia" text,
	"Código de Producto" text, "Tipo de Detalle Pedido" text,
	"Procedencia Producto" text, "ORIGEN | Número de Pedido" text,
	"Código de Oficina" integer, "Código de Ciudad" integer,
	"Código de Departamento" integer,
	"Código de País" integer, "Total Neto" integer,
	"Total IVA" integer, "Total de Descuentos" integer,
	"Total Bruto" integer, "Facturado" text,
	"Estado del Pedido" text, "Tipo de Facturación" text,
	"Clase de Pedido" text, "Tipo de Pedido" text,
	"FECHA_PEDIDO" date, "Despacho | NIT" integer,
	"Número de Pedido" integer, "ID" text,
	"TIPO" text, "CORREO" text, "ID_SITIO" integer,
	"ID_SEGMENTO" text, "FECHA_CXENSE" date,
	"URL" text, "CATEGORIA" text, "SUBCATEGORIA" text,
	"ARTICULO" text, "REFERRERURL" text,
	"CITY" text, "OS" text, "BROWSER" text,
	"BROWSERVERSION" text, "RESOLUTION" text,
	"FECHA_CREACION" date, "FECHA_ACTUALIZACION" date,
	"REGISTRO" text, "ACTIVETIME" integer
  )


-----------------------------------------------------------------------------

SELECT pg_typeof(name)
FROM language
LIMIT 1;



select * from event_schema;
select * from property_event_schema WHERE event='1';


    IF EXISTS (SELECT FROM orders) THEN
        DELETE FROM orders;
    ELSE
        INSERT INTO orders VALUES (1,2,3);
    END IF;
--------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION MAIN()
AS $BODY$
DECLARE
    rec record;
    rec_ record;
BEGIN
    FOR rec IN select * from property_event_schema WHERE event='1' LOOP
		FOR rec_ IN select * from event_property WHERE event_id='304119' LOOP
		    IF pg_typeof(rec.value) == pg_typeof(rec_.value) THEN
		        RAISE NOTICE 'equal';
            ELSE
                RAISE NOTICE 'rec id: %', rec.id;
                RAISE NOTICE 'rec_id: %', rec_.id;
            END IF;
		END LOOP;
	END LOOP;
END
$$
LANGUAGE PLPGSQL;








---------------------------------------------------------------------------------------

SELECT
  event_property.id,
  event_property.event_id,
  event_property.name,
  event_property.value,
  user_event.id,
  user_event.name,
  user_event.created_at,
  user_event.updated_at,
  user_event.valid,
  user_event.user_id
FROM
  event_property
  INNER JOIN user_event ON event_property.event_id = user_event.id
WHERE
  user_event.id in (
    SELECT
      id
    FROM
      user_event
    WHERE
      name = 'Review made'
      AND migrated = false
    limit
      5000
  );


select * from user_event limit 500;


SELECT
  event_property.id,
  event_property.event_id,
  event_property.name,
  event_property.value,
  user_event.name,
  user_event.created_at,
  user_event.updated_at,
  user_event.valid,
  user_event.user_id
FROM
  event_property
  INNER JOIN user_event ON event_property.event_id = user_event.id
WHERE
  user_event.id in (
    SELECT
      id
    FROM
      user_event
    WHERE
      name = 'Paga cuota'
      AND migrated = false
    limit
      5000
  )
ORDER BY
  user_event.id;



select * from user_event where id = 9550;
