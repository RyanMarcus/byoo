# Generated from predicate.g4 by ANTLR 4.7.1
# encoding: utf-8
from antlr4 import *
from io import StringIO
from typing.io import TextIO
import sys

def serializedATN():
    with StringIO() as buf:
        buf.write("\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\f")
        buf.write("!\4\2\t\2\4\3\t\3\3\2\3\2\3\2\3\2\3\2\3\2\5\2\r\n\2\3")
        buf.write("\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\27\n\3\3\3\3\3\3\3")
        buf.write("\7\3\34\n\3\f\3\16\3\37\13\3\3\3\2\3\4\4\2\4\2\2\2\"\2")
        buf.write("\f\3\2\2\2\4\26\3\2\2\2\6\7\7\13\2\2\7\b\7\7\2\2\b\r\7")
        buf.write("\13\2\2\t\n\7\13\2\2\n\13\7\7\2\2\13\r\7\t\2\2\f\6\3\2")
        buf.write("\2\2\f\t\3\2\2\2\r\3\3\2\2\2\16\17\b\3\1\2\17\27\5\2\2")
        buf.write("\2\20\21\7\6\2\2\21\27\5\4\3\5\22\23\7\3\2\2\23\24\5\4")
        buf.write("\3\2\24\25\7\4\2\2\25\27\3\2\2\2\26\16\3\2\2\2\26\20\3")
        buf.write("\2\2\2\26\22\3\2\2\2\27\35\3\2\2\2\30\31\f\3\2\2\31\32")
        buf.write("\7\b\2\2\32\34\5\4\3\4\33\30\3\2\2\2\34\37\3\2\2\2\35")
        buf.write("\33\3\2\2\2\35\36\3\2\2\2\36\5\3\2\2\2\37\35\3\2\2\2\5")
        buf.write("\f\26\35")
        return buf.getvalue()


class predicateParser ( Parser ):

    grammarFileName = "predicate.g4"

    atn = ATNDeserializer().deserialize(serializedATN())

    decisionsToDFA = [ DFA(ds, i) for i, ds in enumerate(atn.decisionToState) ]

    sharedContextCache = PredictionContextCache()

    literalNames = [ "<INVALID>", "'('", "')'", "<INVALID>", "'not'" ]

    symbolicNames = [ "<INVALID>", "<INVALID>", "<INVALID>", "STRING_LITERAL", 
                      "NOT", "C_OP", "L_OP", "LITERAL", "TABLE", "COL", 
                      "WS" ]

    RULE_term = 0
    RULE_predicate = 1

    ruleNames =  [ "term", "predicate" ]

    EOF = Token.EOF
    T__0=1
    T__1=2
    STRING_LITERAL=3
    NOT=4
    C_OP=5
    L_OP=6
    LITERAL=7
    TABLE=8
    COL=9
    WS=10

    def __init__(self, input:TokenStream, output:TextIO = sys.stdout):
        super().__init__(input, output)
        self.checkVersion("4.7.1")
        self._interp = ParserATNSimulator(self, self.atn, self.decisionsToDFA, self.sharedContextCache)
        self._predicates = None



    class TermContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def COL(self, i:int=None):
            if i is None:
                return self.getTokens(predicateParser.COL)
            else:
                return self.getToken(predicateParser.COL, i)

        def C_OP(self):
            return self.getToken(predicateParser.C_OP, 0)

        def LITERAL(self):
            return self.getToken(predicateParser.LITERAL, 0)

        def getRuleIndex(self):
            return predicateParser.RULE_term

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterTerm" ):
                listener.enterTerm(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitTerm" ):
                listener.exitTerm(self)




    def term(self):

        localctx = predicateParser.TermContext(self, self._ctx, self.state)
        self.enterRule(localctx, 0, self.RULE_term)
        try:
            self.state = 10
            self._errHandler.sync(self)
            la_ = self._interp.adaptivePredict(self._input,0,self._ctx)
            if la_ == 1:
                self.enterOuterAlt(localctx, 1)
                self.state = 4
                self.match(predicateParser.COL)
                self.state = 5
                self.match(predicateParser.C_OP)
                self.state = 6
                self.match(predicateParser.COL)
                pass

            elif la_ == 2:
                self.enterOuterAlt(localctx, 2)
                self.state = 7
                self.match(predicateParser.COL)
                self.state = 8
                self.match(predicateParser.C_OP)
                self.state = 9
                self.match(predicateParser.LITERAL)
                pass


        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.exitRule()
        return localctx

    class PredicateContext(ParserRuleContext):

        def __init__(self, parser, parent:ParserRuleContext=None, invokingState:int=-1):
            super().__init__(parent, invokingState)
            self.parser = parser

        def term(self):
            return self.getTypedRuleContext(predicateParser.TermContext,0)


        def NOT(self):
            return self.getToken(predicateParser.NOT, 0)

        def predicate(self, i:int=None):
            if i is None:
                return self.getTypedRuleContexts(predicateParser.PredicateContext)
            else:
                return self.getTypedRuleContext(predicateParser.PredicateContext,i)


        def L_OP(self):
            return self.getToken(predicateParser.L_OP, 0)

        def getRuleIndex(self):
            return predicateParser.RULE_predicate

        def enterRule(self, listener:ParseTreeListener):
            if hasattr( listener, "enterPredicate" ):
                listener.enterPredicate(self)

        def exitRule(self, listener:ParseTreeListener):
            if hasattr( listener, "exitPredicate" ):
                listener.exitPredicate(self)



    def predicate(self, _p:int=0):
        _parentctx = self._ctx
        _parentState = self.state
        localctx = predicateParser.PredicateContext(self, self._ctx, _parentState)
        _prevctx = localctx
        _startState = 2
        self.enterRecursionRule(localctx, 2, self.RULE_predicate, _p)
        try:
            self.enterOuterAlt(localctx, 1)
            self.state = 20
            self._errHandler.sync(self)
            token = self._input.LA(1)
            if token in [predicateParser.COL]:
                self.state = 13
                self.term()
                pass
            elif token in [predicateParser.NOT]:
                self.state = 14
                self.match(predicateParser.NOT)
                self.state = 15
                self.predicate(3)
                pass
            elif token in [predicateParser.T__0]:
                self.state = 16
                self.match(predicateParser.T__0)
                self.state = 17
                self.predicate(0)
                self.state = 18
                self.match(predicateParser.T__1)
                pass
            else:
                raise NoViableAltException(self)

            self._ctx.stop = self._input.LT(-1)
            self.state = 27
            self._errHandler.sync(self)
            _alt = self._interp.adaptivePredict(self._input,2,self._ctx)
            while _alt!=2 and _alt!=ATN.INVALID_ALT_NUMBER:
                if _alt==1:
                    if self._parseListeners is not None:
                        self.triggerExitRuleEvent()
                    _prevctx = localctx
                    localctx = predicateParser.PredicateContext(self, _parentctx, _parentState)
                    self.pushNewRecursionContext(localctx, _startState, self.RULE_predicate)
                    self.state = 22
                    if not self.precpred(self._ctx, 1):
                        from antlr4.error.Errors import FailedPredicateException
                        raise FailedPredicateException(self, "self.precpred(self._ctx, 1)")
                    self.state = 23
                    self.match(predicateParser.L_OP)
                    self.state = 24
                    self.predicate(2) 
                self.state = 29
                self._errHandler.sync(self)
                _alt = self._interp.adaptivePredict(self._input,2,self._ctx)

        except RecognitionException as re:
            localctx.exception = re
            self._errHandler.reportError(self, re)
            self._errHandler.recover(self, re)
        finally:
            self.unrollRecursionContexts(_parentctx)
        return localctx



    def sempred(self, localctx:RuleContext, ruleIndex:int, predIndex:int):
        if self._predicates == None:
            self._predicates = dict()
        self._predicates[1] = self.predicate_sempred
        pred = self._predicates.get(ruleIndex, None)
        if pred is None:
            raise Exception("No predicate with index:" + str(ruleIndex))
        else:
            return pred(localctx, predIndex)

    def predicate_sempred(self, localctx:PredicateContext, predIndex:int):
            if predIndex == 0:
                return self.precpred(self._ctx, 1)
         




