// usage: q schemachecker.q [-schema schemafile.csv] [-bit64 0|1] [-debug 0|1] [-nocheck 0|1] [-discard 0|1]
// -schema  : schema file to load
// -bit64   : assume 64 bit version, used for some sizings
// -debug   : print on each insert
// -nocheck : don't check incoming data, just insert it
// -discard : discard incoming data, just count it

\d .schema

params:.Q.def[`schema`bit64`debug`nocheck`discard!(`;1b;0b;0b;0b)] .Q.opt .z.x	
bit64:params[`bit64]	/ bit version - default 64bit
debug:params[`debug]	/ debug mode - default 0b

if[0i~system"p";system"p 9990"]		/ set the port if not set

// printe messages if running in debug mode
.z.ps:.z.pg:{if[debug; -1"received message: ",-3!x]; value x}

// table to store the schemas
schemas:([]table:`symbol$(); col:`symbol$(); coltype:`symbol$(); isnested:`boolean$();expectedtype:`char$();nestedcount:`long$(); tablecount:`long$())
kdbtypes:`boolean`guid`byte`short`int`long`real`float`char`symbol`timestamp`month`date`datetime`timespan`minute`second`time`blob!"BGXHIJEFCSPMDZNUVT "
kdbsizes:key[kdbtypes]!1 16 1 2 4 8 4 8 1 4 8 4 4 8 8 4 4 4 40
kdbsizes[`symbol]:kdbsizes[`symbol]*1+bit64

// add a schema to the test harness
addschema:{ 
 
 if[not all `table`col`coltype`isnested in cols x; '"missing columns: you need to supply table (symbol), col (symbol), coltype (symbol), isnested (boolean)"];
 if[count weirdtypes:select from x where not coltype in key .schema.kdbtypes; '"invalid column types supplied: "," " sv string exec coltype from weirdtypes]; 
 
 // add the new schema, remove any old reference
 delete from `.schema.schemas where table in exec table from x;
 // add in expected type, allow there to be a blob type
 .schema.schemas,:update expectedtype:@[.schema.kdbtypes[coltype];where not isnested;lower] from x;
 
 // set the schema for each table 
 {@[`.;x;:;buildempty x]} each exec distinct table from x; 
 }

// build an empty table from the supplied tablename
buildempty:{
 if[0=count tobuild:select from schemas where table=x; '"table not defined in schema table"];
 // generate the list of types
 typelist:("B"^kdbtypes tobuild`coltype)$\:" ";
 typelist:@[typelist; w; :;(count w:where (tobuild`isnested) or null tobuild`expectedtype)#enlist()];
 // return the table
 0#enlist (tobuild`col)!typelist 
 }

// read in a schema from a file
readschema:{
 requiredcols:`table`col`coltype`isnested;
 tab:("SSSBJJ";enlist",")0:x;
 // check required columns are all there
 if[not all requiredcols in cols tab; '"CSV is missing columns.  Should contain required columns of ",(" " sv string requiredcols)," with optional columns of nestedcount and tablecount"]; 
 // make sure the last two columns have the correct names
 tab:(requiredcols,`nestedcount`tablecount) xcol tab;
 // check for nulls
 if[any s:sum null tab requiredcols; '"null values found at row(s) "," " sv string 1+where s];
 // add the read in values to the schema table
 addschema tab 
 
 }

checkinsert:{[tab; data]
 
 // check length
 if[not 1=count distinct c:count each data; '"ragged lists received.  All lengths should be the same.  Lengths are "," " sv string c];   
 
 // the schema to check against
 if[0=count tocheck:select from schemas where table=tab;'"supplied table ",(string tab)," doesn't have a schema set up"];
 
 // peg on a time column if required
 if[count[data]=-1+count tocheck; data:(enlist (count first data)#.z.p),data];
 if[not count[data]=count tocheck; '"incorrect column length received.  Received data is ",-3!data];

 // build the table to insert
 toinsert:flip tocheck[`col]!data;

 if[count wrongtypes:select col:c,receivedtype:t,expectedtype from (meta[toinsert] lj 1!select c:col, expectedtype from tocheck) where not (t=expectedtype) or null expectedtype;
  show wrongtypes;
  '"incorrect type sent"]; 
 
 // check all the nested types are consistent
 if[any nq:1<count each distinct each type each' toinsert nt:exec col from tocheck where isnested; 
  '"nested types are not consistent: ",-3!(nt where nq)#toinsert]; 
 
 .[insert;(tab;toinsert);{[x;e] '"failed to insert data - ",e}[toinsert]];
  
 if[debug;-1"insert successful"];
 }

arraysize:{2 xexp ceiling xlog[2;16+x]}
sizesforschema:{
 
 // make sure all the table sizes are the same
 x:update tablecount:1|max tablecount by table from x;

 // make sure nested count is at least 1 for nested columns
 x:update nestedcount:1|nestedcount from x;
 
 // nesteddatasize is the size of an individual element in the nested array
 x:update nesteddatasize:.schema.arraysize nestedcount*.schema.kdbsizes[coltype] from x;
 // datasize is the size of the full vector (nested or otherwise)
 x:update datasize:?[isnested;nesteddatasize*tablecount;.schema.arraysize tablecount*.schema.kdbsizes[coltype]] from x;
 // nested pointers is the size of the nested pointer list
 x:update nestedpointers:.schema.arraysize tablecount*4+4*.schema.bit64 from x;
 // total vector size is the size of each vector
 x:update totalvectorsizeMB:`long$(datasize+nestedpointers*isnested)%2 xexp 20 from x; 
 x
 }

// utility function to see the size of each column individually
size:{delete nesteddatasize,datasize,nestedpointers from sizesforschema[.schema.schemas]}

// aggregate size statistics
sizestats:{
 // calc aggregate stats
 stats:select totalsizeMB:`long$sum[totalvectorsizeMB] by val:table from .schema.size[];
 // add in a total
 stats,([val:enlist`TOTALSIZE]totalsizeMB:value sum stats)}

// upd function used in nocheck mode
// data is not validated, just inserted
// assume that we need to add the time as the first column on every insert
// (modify this function if not)
nocheckupd:{[t;x]
 t insert (enlist (count first x)#.z.p),x;
 }

// upd function used in discard mode
// data is just counted and then discarded (so the only kdb+ bit is the IPC de-serialisation)
discardcount:0 
discardupd:{[t;x] discardcount+::count first x};

// set .u.upd to be equal to checkinsert, to simulate the tickerplant
.u.upd:checkinsert

if[params[`nocheck]; .u.upd:nocheckupd];
if[params[`discard]; .u.upd:discardupd];
// reset message handlers in nocheck and discard case
if[max params[`nocheck`discard]; 
  system"x .z.pg";
  system"x .z.ps"];

if[not null file:params[`schema]; readschema hsym file]

\
addschema ([]table:`trade;col:`time`sym`notes`price`orderid;coltype:`timestamp`symbol`char`float`char;isnested:00101b)
.u.upd[`trade;(`a`a;("note1";"note2");200 300f;("o1";"o2"))]			/correct
.u.upd[`trade;(`a`a;("note1";"note2");200 300;("o1";"o2"))] 			/wrong type (simple type)
.u.upd[`trade;(`a`a;("note1";"note2");200 300f;(8 9 10;8 9))]			/wrong type (nested type)
.u.upd[`trade;(`a`a;("note1";"note2");200 300f;(();()))]			/wrong type (non typed nested)
.u.upd[`trade;(`a`a`c;("note1";"note2");200 300f;(();()))]              	/ragged lists
.u.upd[`trade;(`a`a;("note1";"note2");("o1";"o2"))]              		/not enough columns
.u.upd[`trade;(`a`a;("note1";"note2");200 300f;("o1";"o2");4 5;20 30)]  	/too many columns
.u.upd[`newstuff;(`a`a;("note1";"note2");200 300f;("o1";"o2"))]			/table not defined
.u.upd[`trade;(2#2000.01.01D;`a`a;("note1";"note2");200 300f;("o1";"o2"))]     	/correct, time data supplied
.u.upd[`trade;(`a`a;("note1";"note2");200 300f;("o1";3 4 5))]			/nested types not consistent
