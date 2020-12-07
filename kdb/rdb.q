\c 1000 1000
\C 1000 1000

upd:insert;

\d .orig

/VOD.L   150 156;  `XLON
/HEIN.AS 100 105;  `XAMS
/JUVE.MI 1230 1240;`XMIL
tickers:((`VOD.L;150 + til 6;`XLON);(`HEIN.AS;100 + til 5;`XAMS);(`JUVE.MI;1230 + til 10;`XMIL));

quote:`time xasc flip `time`sym`bid`bsize`ask`asize`bex`aex!flip raze flip each {(.z.d+x?.z.n;x#y 0;x?y 1;1000+x?49000;x?y 1;1000+x?49000;x#y 2;x#y 2)}[3000;] each tickers;
trade:`time xasc flip `time`sym`price`size`ex!flip raze flip each {(.z.d+x?.z.n;x#y 0;x?y 1;1000+x?49000;x#y 2)}[50000;] each tickers;

tableList:`trade`quote`staticTrade`staticQuote;

\d .

.z.pw:{[u;p]
    (u;p)~(`username;"password")
    };
    
.z.po:{[x]
    -1@string[.z.p],"|INF|  open : ",("0"^-4$string[.last.w:x]);
    .last.po:x;
    };
    
.z.pc:{[x]
    -1@string[.z.p],"|INF| close : ",("0"^-4$string[.last.w:x]);
    .last.pc:x;
    };

.z.ps:{[x]
    $[not 10=type x;();"value(`upd"~10#x;:value x;()];
    -1@string[.z.p],"|INF| async : ",("0"^-4$string[.last.w:.z.w])," : ",.Q.s1 .last.ps:x;
    neg[.z.w] value x;
    };

.z.pg:{[x]
    -1@string[.z.p],"|INF|  sync : ",("0"^-4$string[.last.w:.z.w])," : ",.Q.s1 .last.pg:x;
    value x
    };

\d .aqrest

// Function to filter queries according to user roles
// table (type table), from tablelist
// roles (type symbol), list of roles to be applied to user query
filterTable:{[table;roles]
    augtable:0#table;
    // Tables filter - exit with 0#table if disabled
    if[((table~get `..trade)|(table~get `..staticTrade)) & `perms.tables.no_trade in roles; :0#augtable];
    if[((table~get `..quote)|(table~get `..staticQuote)) & `perms.tables.no_quote in roles; :0#augtable];
    // Rows filter - select from table with max delay found
    rowRoles:(`perms.rows.realtime; `perms.rows.delay_05; `perms.rows.delay_15);
    rowValue:(00:00; 00:05; 00:15);
    if[any idx:rowRoles in roles;
        augtable:select from table where time < .z.p - rowValue last where idx;
        ];
    // Syms filter - select from augmented where syms match - all if not specified
    symRoles:(`perms.sym.xlon; `perms.sym.xams; `perms.sym.xmil);
    symValue:("*.L"; "*.AS"; "*.MI");
    if[any idx:symRoles in roles;
        augtable:select from augtable where any sym like/:symValue where idx;
        ];
    // Columns filter - remove any columns
    if[`perms.cols.no_ex in roles;
        augtable:flip `ex`aex`bex _ flip augtable;
        ];
    augtable
    };

// Function which returns result of the query after applying filter
// query (type string), query sent by user, can be qSQL or functional form
// roles (type symbol), list of roles to be applied to user query
applyRole:{[query;roles]
    blocked_q:("exit";"system";"hopen";"hclose");
    blocked_k:("\\\\";"<:";">:");
    if[any idx:1=count each ss[query;] each blocked_q,blocked_k;
        '"blocked : ",","sv (blocked_q,blocked_k) where idx;
        ];
    ssrTab:{[x;y;z] ssr[x;string y;".aqrest.filterTable[",string[y],";",(","sv .Q.s1 each z),"]"] };
    reval parse .last.qs:ssrTab[;;roles]/[;.orig.tableList] ssr/[query;].(.Q.s1';string')@\:.orig.tableList
    };

// Function to be called by q-Rest java server
// query (type string), query sent by q-Rest server
// metaDict (type dict), meta sent by q-Rest server
execute:{[query;metaDict]
    -1@string[.z.p],"|INF|  exec : ",("0"^-4$string[.last.w:.z.w])," : ",.Q.s1 query;
    `status`result!@[{(1b;applyRole . x)};(query;(),`$" " vs string metaDict[`roles]);{(0b;"error: ",x)}]
    };

\d .

staticTrade:.orig.trade;
staticQuote:.orig.quote;

trade:0#.orig.trade;
quote:0#.orig.quote;
