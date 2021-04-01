package org.imsi.queryEREngine.imsi.er.Utilities;

import org.imsi.queryEREngine.imsi.er.MetaBlocking.EdgePruning;

public class MetaBlockingConfiguration {
    public enum PruningAlgorithm {
        CEP,
        CNP,
        WEP,
        WNP
    }
    
    public enum WeightingScheme {
        ARCS,
        CBS,
        ECBS,
        JS,
        EJS
    }
    
    public static AbstractMetablocking getModel (PruningAlgorithm algorithm, WeightingScheme scheme) {
        switch (algorithm) {
//            case CEP:
//                return new TopKEdges(scheme);
//            case CNP:
//                return new KNearestEntities(scheme);
            case WEP:
                return new EdgePruning(scheme);
//            case WNP:
//                return new NodePruning(scheme);
            default:
                return null;    
        }
    }
}
