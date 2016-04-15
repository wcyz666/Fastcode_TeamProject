package util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ASUA on 2016/4/14.
 */
public class CombinationGenerator {
    private static void combineHelper(String[] data, List<List<String>> lists, List<String> list, int cur, int n, int k) {
        if (list.size() == k) {
            lists.add(new ArrayList<>(list));
        } else {
            for (int i = cur; i < n; i++) {
                list.add(data[i]);
                combineHelper(data, lists, list, i + 1, n, k);
                list.remove(list.size() - 1);
            }
        }
    }

    public static List<List<String>> combine(String[] data, int n, int k) {
        List<List<String>> lists = new ArrayList<>();
        combineHelper(data, lists, new ArrayList<String>(), 0, n, k);

        return lists;
    }
}
