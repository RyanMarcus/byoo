import json

with open("operators.json") as f:
    OPERATORS = json.load(f)

class UnknownByooOperatorError(Exception):
    def __init__(self, op):
        super().__init__("Unknown byoo operator requested: {}".format(op))

class InvalidChildrenError(Exception):
    def __init__(self, op, expected):
        super().__init__("Operator {} was expecting {} children"
                         .format(op, expected))
        
class OperatorFileError(Exception):
    def __init__(self, op, expected):
        super().__init__("Operator {} was {} a file"
                         .format(op, "expecting" if expected
                                 else "not expecting"))
    
class ByooOperatorBuilder:
    def __init__(self):
        pass

    def __getattr__(self, attr):
        attr = attr.replace("_", " ")
        if attr not in OPERATORS:
            raise UnknownByooOperatorError(attr)

        def closure(*args):
            to_r = ByooOperator(attr)
            to_r.set_children(args)
            return to_r
        
        return closure

class ByooOperator:
    def __init__(self, op):
        self.__op = op
        self.__children = []
        self.__options = {}

    def set_children(self, children):
        cc = OPERATORS[self.__op]["child count"]
        if cc == "none":
            if children:
                raise InvalidChildrenError(self.__op, cc)
        elif cc == "any":
            # all good
            pass
        else:
            cc = int(cc)
            if cc != len(children):
                raise InvalidChildrenError(self._op, cc)
            
        self.__children = children

    def types(self, type_string):
        if isinstance(type_string, str):
            types = []
            for c in type_string:
                if c == "i":
                    types.append("INTEGER")
                elif c == "r":
                    types.append("REAL")
                elif c == "t":
                    types.append("TEXT")
                elif c == "b":
                    types.append("BLOB")
                self.__options["types"] = types
        else:
            self.__options["types"] = type_string
        return self

    def __has_file(self):
        return (OPERATORS[self.__op]["input file"]
                or OPERATORS[self.__op]["output file"])
    
    def file(self, path):
        if not self.__has_file():
            raise OperatorFileError(self.__op, False)
        self.__options["file"] = path
        return self

    def __getattr__(self, attr):
        def closure(arg):
            self.__options[attr] = arg
            return self
        return closure

    def to_json(self):
        if self.__has_file() and "file" not in self.__options:
            raise OperatorFileError(self.__op, True)
        elif not self.__has_file and "file" in self.__options:
            raise OperatorFileError(self.__op, False)
        
        to_r = {"op": self.__op.replace("_", " ")}
        if self.__options:
            to_r["options"] = self.__options

        if self.__children:
            to_r["input"] = [x.to_json() for x in self.__children]
            
        return to_r


if __name__ == "__main__":
    byoo = ByooOperatorBuilder()

    scan1 = (byoo.csv_read()
             .file("res/inputs/test1.csv")
             .types("iitir"))
    scan2 = (byoo.csv_read()
             .file("res/inputs/test2.csv")
             .types("it"))

    hj = (byoo.hash_join(scan1, scan2)
          .left_cols([0])
          .right_cols([0]))

    proj = (byoo.project(hj)
            .cols([0, 5]))

    print(json.dumps(proj.to_json(), indent=2))
