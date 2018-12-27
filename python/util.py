from byoo_operator import ByooOperatorBuilder
from predicate import parse_predicate

byoo = ByooOperatorBuilder()

class Database:
    def __init__(self):
        self.__relations = {}

    def add_relation(self, relation_name, relation_file):
        if relation_name in self.__relations:
            raise ValueError("The relation {} was already in the database"
                             .format(relation_name))

        self.__relations[relation_name] = relation_file


    def reader_for(self, relation_name, columns):
        relation_file = self.__relations[relation_name]
        
        if relation_file.endswith("csv"):
            csv = (byoo.csv_read()
                   .file(relation_file))
            proj = (byoo.project(csv)
                    .cols(columns))
            return proj
        
        elif relation_file.endswith("byoo"):
            readers = [byoo.columnar_read()
                       .file(relation_file)
                       .col(x) for x in columns]
            union = byoo.union(*readers)
            return union

        raise ValueError("Unknown file type for: {}".format(relation_file))


db = Database()
db.add_relation("t1", "res/inputs/test1.csv")
db.add_relation("t2", "res/inputs/test2.csv")

def make_plan_from_join_order(db, join_order, predicates):
    predicates = [parse_predicate(x) for x in predicates]

    # TODO it doesn't make sense to use rel1 and rel2 as ops for
    # join_predicate_for because each join child could be
    # a number of relations
    
    for p in predicates:
        print(p.required_columns())
        print(p.is_join_predicate_for("t1", "t2"))
        print(p.is_equijoin_predicate_for("t1", "t2"))

make_plan_from_join_order(db, ("t1", "t2"), ["t1.0 = t2.1", "t1.3 > 8"])
