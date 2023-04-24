package org.imsi.queryEREngine.imsi.er.Utilities;

//simple union-find based on int[] arrays
//for  "parent" and "rank"
//implements the "disjoint-set forests" described at
//http://en.wikipedia.org/wiki/Disjoint-set_data_structure
//which have almost constant "amortized" cost per operation
//(actually O(inverse Ackermann))

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UnionFind {

    //private int[] _parent;
    //private int[] _rank;
    public HashMap<Integer, Integer> _parent = new HashMap<>();
    private HashMap<Integer, Integer> _rank = new HashMap<>();

    public Map<Integer, Integer> getParent() {
        return _parent;
    }

    public boolean isInSameSet(int a, int b) {
        return find(a) == find(b);
    }

    public int find(int i) {

        if (!_parent.containsKey(i)) {
            _parent.put(i, i);
        }
        int p = _parent.get(i);
        if (i == p) {
            return i;
        }

        int findP = find(p);
        _parent.put(i, findP);

        return _parent.get(i);

    }


    public void union(int i, int j) {

        int root1 = find(i);
        int root2 = find(j);

        if (root2 == root1) return;

        if (!get_rank().containsKey(root1))
            get_rank().put(root1, 0);
        if (!get_rank().containsKey(root2))
            get_rank().put(root2, 0);

        if (get_rank().get(root1) > get_rank().get(root2)) {
            _parent.put(root2, root1);
        } else if (get_rank().get(root2) > get_rank().get(root1)) {
            _parent.put(root1, root2);
        } else {
            _parent.put(root2, root1);
            get_rank().put(root1, get_rank().get(root1) + 1);
        }
    }


    public UnionFind(Set<Integer> set) {

        for (int i : set) {
            makeSet(i);
//            _parent.put(i, i);
//            get_rank().put(i, 0);
        }
    }

    public void makeSet(int x) {
        if (!_parent.containsKey(x)) {
            _parent.put(x, x);
            _rank.put(x, 0);
        }
    }

    public HashMap<Integer, Integer> get_rank() {
        return _rank;
    }


    public void set_rank(HashMap<Integer, Integer> _rank) {
        this._rank = _rank;
    }

}