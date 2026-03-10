## Hemos Generado una classe base a todos los scrapers con el fin the poder calcular como avanzan los difrentes 

SCRAPER es la classe que implementaran todos los scrapers y tenfra la clase que tendran todos. 

Con el fin de usar classes simultaneamente. 

## Hemos Generado un BatchProcessor 

Implementacion de la classe DataExporter con el din de guardar los diferentes elementos correctamente. 

CSV --> POST,  NEWS

JSON --> POST,  NEWS

CSV  --> COMMENTS  

## Hemos Generado un BatchProcessor 

Un Batch procesor tomara a cuenta los procesos de guardar informacion en la base ded datos. 

Tomara como parametro : 

    >  Scraper : Que es la identidad del Scraper que le vamos a pasar tomaremos como reference la clase Scraper padre de todas esas clases inferiores. 

    > Fichero de salida escogido 
    
    > Un DataExporter que es una classe que se dedicara en escribir en los formatos csv o json correspondientes 
    
    > Tendremos dos ficheros para cada agente batch el status y el progress 

LOAD PROGRESS 
    HACE UN JSON LOAD, MOSTARNDO LA INFORMACION QUE TENEMOS 

SAVE PROGRESS 
    GUARDA EL PROGRESS  

SAVE STATS 
    GUARDA LOS STATS 

GET_BATCH_FILENAME 
    DEVUELVE EL PATH DONDE ESTA LOS DATOS 

PROCESS_BATCH 
