grammar predicate;

// https://stackoverflow.com/questions/16045209/antlr-how-to-escape-quote-symbol-in-quoted-string
STRING_LITERAL : '"' (~('"' | '\\' | '\r' | '\n') | '\\' ('"' | '\\'))* '"';

NOT: 'not' ;
C_OP: '=' | '<' | '>' | '<=' | '>=' | '!=' | 'contains' ;
L_OP: 'and' | 'or';
LITERAL: [0-9]+ | [0-9]+ '.' [0-9]+ | STRING_LITERAL ;
TABLE: [A-Z0-9a-z]+ ;
COL: TABLE '.' [0-9]+ ;
WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines

term: COL C_OP COL
    | COL C_OP LITERAL
    ;

predicate: term
    | NOT predicate
    | '(' predicate ')'
    | predicate L_OP predicate
    ;

        
