import java.util.ArrayList;
import java.util.List;

public class Primes {
    // 10^8의 제곱근 = 10000 이하 소수만 있으면 됨
    static final int[] SMALL = makeSmallPrimes(10000);

    static int[] makeSmallPrimes(int limit) {
        boolean[] composite = new boolean[limit + 1];
        List<Integer> list = new ArrayList<>();
        for (int p = 2; p <= limit; p++) {
            if (composite[p]) continue;
            list.add(p);
            for (long m = (long) p * p; m <= limit; m += p) composite[(int) m] = true;
        }
        int[] arr = new int[list.size()];
        for (int i = 0; i < arr.length; i++) arr[i] = list.get(i);
        return arr;
    }

    static int stepLo(int k, int s) {
        return k * 100000 + (s - 1) * 10000 + 1;
    }

    // 구간 [lo, hi]의 소수 개수 (구간 체)
    static int countRange(int lo, int hi) {
        boolean[] notPrime = new boolean[hi - lo + 1];
        for (int p : SMALL) {
            if ((long) p * p > hi) break;
            long start = Math.max((long) p * p, ((long) lo + p - 1) / p * p);
            for (long m = start; m <= hi; m += p) notPrime[(int) (m - lo)] = true;
        }
        if (lo <= 1) notPrime[1 - lo] = true;
        int cnt = 0;
        for (boolean b : notPrime) if (!b) cnt++;
        return cnt;
    }

    static int count(int k, int s) {
        int lo = stepLo(k, s);
        return countRange(lo, lo + 9999);
    }

    // java Primes 로 검증값 확인
    public static void main(String[] args) {
        System.out.println("small primes: " + SMALL.length + ", last = " + SMALL[SMALL.length - 1]);
        System.out.println("count(0,1) = " + count(0, 1));
        System.out.println("[91,110] = " + countRange(91, 110));
        for (int k : new int[]{0, 152, 160, 271}) {
            StringBuilder sb = new StringBuilder("task " + k + ":");
            int acc = 0;
            for (int s = 1; s <= 10; s++) {
                acc += count(k, s);
                sb.append(' ').append(acc);
            }
            System.out.println(sb);
        }
        long total = 0;
        for (int k = 0; k < 1000; k++)
            for (int s = 1; s <= 10; s++) total += count(k, s);
        System.out.println("total = " + total + (total == 5761455 ? " OK" : " WRONG"));
    }
}
