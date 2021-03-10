// load schemachecker file
\l docker/lib/schemachecker.q

.schema.addschema ([]table:`trade;col:`time`chrontime`sym`price`size`ex;coltype:`timestamp`timestamp`symbol`float`float`symbol; isnested:000000b);
.schema.addschema ([]table:`quote;col:`time`chrontime`sym`bid`bsize`ask`asize`bex`aex;coltype:`timestamp`timestamp`symbol`float`float`float`float`symbol`symbol;isnested:000000000b);
