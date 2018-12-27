# Generated from predicate.g4 by ANTLR 4.7.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .predicateParser import predicateParser
else:
    from predicateParser import predicateParser

# This class defines a complete listener for a parse tree produced by predicateParser.
class predicateListener(ParseTreeListener):

    # Enter a parse tree produced by predicateParser#term.
    def enterTerm(self, ctx:predicateParser.TermContext):
        pass

    # Exit a parse tree produced by predicateParser#term.
    def exitTerm(self, ctx:predicateParser.TermContext):
        pass


    # Enter a parse tree produced by predicateParser#predicate.
    def enterPredicate(self, ctx:predicateParser.PredicateContext):
        pass

    # Exit a parse tree produced by predicateParser#predicate.
    def exitPredicate(self, ctx:predicateParser.PredicateContext):
        pass


