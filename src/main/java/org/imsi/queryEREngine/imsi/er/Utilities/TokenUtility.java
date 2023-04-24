package org.imsi.queryEREngine.imsi.er.Utilities;

import org.imsi.queryEREngine.apache.calcite.rex.RexCall;
import org.imsi.queryEREngine.apache.calcite.rex.RexLiteral;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexVisitorImpl;

import java.util.*;
import java.util.stream.Collectors;

public class TokenUtility {


    private static class TokenVisitor extends RexVisitorImpl<RexLiteral> {
        private String token;

        protected TokenVisitor() {
            super(true);
        }

        public String getToken() {
            return token;
        }


        @Override public RexLiteral visitLiteral(RexLiteral literal) {
            this.token = literal.toString();
            return literal;
        }

    }

    public static  <T> List<T> flattenListOfListsImperatively(
            List<List<T>> nestedList) {
        List<T> ls = new ArrayList<>();
        nestedList.forEach(ls::addAll);
        return ls;
    }


    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    public static List<String> getTokens(List<RexNode> conjuctions){
        List<List<String>> allTokens = new ArrayList<>();
        Integer betweenBeginInt = 0;
        Integer betweenEndInt = 0;
        if(conjuctions == null) return flattenListOfListsImperatively(allTokens);
        for(RexNode condition : conjuctions) {
            String kind = condition.getKind().toString();
            final RexCall rexCall = (RexCall) condition;
            TokenVisitor tokenVisitor = new TokenVisitor();
            rexCall.accept(tokenVisitor);
            List<String> tokens = new ArrayList<>();
            String token = tokenVisitor.getToken();
            if(kind.equals("OR")) {
                List<String> disjTokens = new ArrayList<>();
                for(RexNode disjCondition : rexCall.getOperands()) {
                    final RexCall rexCallConj = (RexCall) disjCondition;
                    TokenVisitor tokenVisitorConj = new TokenVisitor();
                    rexCallConj.accept(tokenVisitorConj);
                    String disjToken = tokenVisitorConj.getToken();
                    disjToken = disjToken.replace("%", "").replace(":VARCHAR", "").replace("'", "").toLowerCase();
                    disjTokens.add(disjToken);
                }
            }
            else {
                switch(kind) {
                    case("LIKE"):
                        token = token.replace("%", "").replace("'", "").toLowerCase();
                        //if its more than 1 tokens
                        if(token.contains(" "))
                            tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
                        else
                            tokens.add(token);
                        break;
                    case("GREATER_THAN"):
                        token = token.replace("'", "").toLowerCase();
                        if(isNumeric(token))
                            betweenBeginInt = Integer.parseInt(token) + 1;
                        break;
                    case("GREATER_THAN_OR_EQUAL"):
                        token = token.replace("'", "").toLowerCase();
                        if(isNumeric(token))
                            betweenBeginInt = Integer.parseInt(token);
                        break;
                    case("LESS_THAN"):
                        token = token.replace("'", "").toLowerCase();
                        if(isNumeric(token))
                            betweenEndInt = Integer.parseInt(token) - 1;
                        break;
                    case("LESS_THAN_OR_EQUAL"):
                        token = token.replace("'", "").toLowerCase();
                        if(isNumeric(token))
                            betweenEndInt = Integer.parseInt(token);
                        break;
                    case("EQUALS"):
                        token = token.replace("'", "").replace(":VARCHAR", "").toLowerCase();
                        if(token.contains(" "))
                            tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
                        else
                            tokens.add(token);
                        break;
                    case("OR"):
                        System.out.println(token);
                        break;
                    default:
                        token = token.replace("'", "").replace(":varchar", "").toLowerCase();
                        if(token.contains(" "))
                            tokens.addAll(Arrays.stream(token.split(" ")).collect(Collectors.toList()));
                        else
                            tokens.add(token);
                        break;
                }
                if(!tokens.isEmpty()) allTokens.add(tokens);
            }
        }
        List<String> tokens = new ArrayList<>();

        if(betweenEndInt != 0 && betweenBeginInt != 0) {
            for(int i = betweenBeginInt; i <= betweenEndInt; i ++) {
                tokens.add(Integer.toString(i));
            }
        }
        else if(betweenEndInt != 0 && betweenBeginInt == 0) {
            for(int i = 0; i <= betweenEndInt; i ++) {
                tokens.add(Integer.toString(i));
            }
        }
        else if(betweenEndInt == 0 && betweenBeginInt != 0) {
            for(int i = betweenBeginInt; i <= 100; i ++) {
                tokens.add(Integer.toString(i));
            }
        }
        if(!tokens.isEmpty()) allTokens.add(tokens);
        return flattenListOfListsImperatively(allTokens);
    }

}

