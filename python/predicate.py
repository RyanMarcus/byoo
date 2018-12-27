from antlr4 import *
import json
from grammer.predicateListener import predicateListener
from grammer.predicateLexer import predicateLexer
from grammer.predicateParser import predicateParser

def op_str_to_byoo(op):
    if op == "=":
        return "eq"
    elif op == ">":
        return "gt"
    elif op == ">=":
        return "gte"
    elif op == "<":
        return "lt"
    elif op == "<=":
        return "lte"
    elif op == "contains":
        return "contains"

class MaterializePredicateListener(predicateListener):
    def __init__(self):
        self.__root = {"children": []}
        self.__stack = [self.__root]
    
    def enterTerm(self, term):
        obj = {"op": op_str_to_byoo(str(term.C_OP()))}
        if len(term.COL()) == 2:
            obj["col1"] = str(term.COL(0))
            obj["col2"] = str(term.COL(1))
        else:
            obj["col"] = str(term.COL(0))
            obj["val"] = str(term.LITERAL())

        self.__stack[-1]["children"].append(obj)

    def exitTerm(self, term):
        pass

    def enterPredicate(self, pred):
        if pred.term():
            return
        
        if pred.L_OP() or pred.NOT():
            op = "not" if pred.NOT() else str(pred.L_OP())
            obj = {"op": op,
                   "children": []}
                
            self.__stack[-1]["children"].append(obj)
            self.__stack.append(obj)
            return
        
        # ignore the case of just ( predicate ).
        return

    def exitPredicate(self, pred):
        if pred.L_OP() or pred.NOT():
            self.__stack = self.__stack[:-1]

    def get(self):
        return self.__root["children"][0]


class Predicate:
    def __init__(self, data):
        self.__data = data

    def required_columns(self):
        req_cols = set()
        def extract(root):
            for key in ["col", "col1", "col2"]:
                if key in root:
                    req_cols.add(root[key])
            if "children" in root:
                for child in root:
                    extract(child)
                    
        extract(self.__data)
        return req_cols

    def is_join_predicate_for(self, rel1, rel2):
        req_cols = self.required_columns()
        req_tables = set()
        for cols in req_cols:
            table, _ = cols.split(".")
            req_tables.add(table)

        return req_tables == set([rel1, rel2])
    
    def is_equijoin_predicate_for(self, rel1, rel2):
        if not self.is_join_predicate_for(rel1, rel2):
            return False

        return self.__data["op"] == "eq"

    def to_json(self):
        return json.dumps(self.__data)


def parse_predicate(pred_str):
    lexer = predicateLexer(InputStream(pred_str))
    stream = CommonTokenStream(lexer)
    parser = predicateParser(stream)
    tree = parser.predicate()
    
    printer = MaterializePredicateListener()
    walker = ParseTreeWalker()
    walker.walk(printer, tree)
    return Predicate(printer.get())

if __name__ == "__main__":
    parse_predicate("t1.5 = 6 or t1.5 = 7 and not (t1.7 > t2.7 or t1.8 < t1.9)")
