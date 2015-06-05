package org.broadinstitute.hellbender.utils;

import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.special.Gamma;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.util.Arrays;
import java.util.List;

/**
 * MathUtils is a static class (no instantiation allowed!) with some useful math methods.
 */
public final class MathUtils {

    /**
     * The smallest log10 value we'll emit from normalizeFromLog10 and other functions
     * where the real-space value is 0.0.
     */
    public static final double LOG10_P_OF_ZERO = -1000000.0;

    /**
     * Log10 of the e constant.
     */
    public static final double LOG10_OF_E = Math.log10(Math.E);

    /**
     * Private constructor.  No instantiating this class!
     */
    private MathUtils() {
    }

    public static int arrayMaxInt(final List<Integer> array) {
        if (array == null)
            throw new IllegalArgumentException("Array cannot be null!");
        if (array.size() == 0)
            throw new IllegalArgumentException("Array size cannot be 0!");

        int m = array.get(0);
        for (int e : array)
            m = Math.max(m, e);
        return m;
    }

    /**
     * A helper class to maintain a cache of log10 values
     */
    public static class Log10Cache {
        /**
         * Get the value of log10(n), expanding the cache as necessary
         * @param n operand
         * @return log10(n)
         */
        public static double get(final int n) {
            if (n < 0)
                throw new GATKException(String.format("Can't take the log of a negative number: %d", n));
            if (n >= cache.length)
                ensureCacheContains(Math.max(n+10, 2*cache.length));
            /*
               Array lookups are not atomic.  It's possible that the reference to cache could be
               changed between the time the reference is loaded and the data is fetched from the correct
               offset.  However, the value retrieved can't change, and it's guaranteed to be present in the
               old reference by the conditional above.
             */
            return cache[n];
        }

        /**
         * Ensures that the cache contains a value for n.  After completion of ensureCacheContains(n),
         * #get(n) is guaranteed to return without causing a cache expansion
         * @param n desired value to be precomputed
         */
        public static void ensureCacheContains(final int n) {
            if (n < cache.length)
                return;
            final double[] newCache = new double[n + 1];
            System.arraycopy(cache, 0, newCache, 0, cache.length);
            for (int i=cache.length; i < newCache.length; i++)
                newCache[i] = Math.log10(i);
            cache = newCache;
        }

        //initialize with the special case: log10(0) = NEGATIVE_INFINITY
        private static double[] cache = new double[] { Double.NEGATIVE_INFINITY };
    }

    // A fast implementation of the Math.round() method.  This method does not perform
    // under/overflow checking, so this shouldn't be used in the general case (but is fine
    // if one is already make those checks before calling in to the rounding).
    public static int fastRound(final double d) {
        return (d > 0.0) ? (int) (d + 0.5d) : (int) (d - 0.5d);
    }

    public static double sum(final double[] values) {
        double s = 0.0;
        for (double v : values)
            s += v;
        return s;
    }

    public static long sum(final int[] x) {
        long total = 0;
        for (int v : x)
            total += v;
        return total;
    }

    public static int sum(final byte[] x) {
        int total = 0;
        for (byte v : x)
            total += (int)v;
        return total;
    }

    public static long sum(final long[] x) {
        int total = 0;
        for (long v : x)
            total += v;
        return total;
    }

    /** Returns the sum of the elements in the array starting with start and ending before stop. */
    public static long sum(final long[] arr, final int start, final int stop) {
        return sum(Arrays.copyOfRange(arr, start, stop));
    }

    /**
     * Compares double values for equality (within 1e-6), or inequality.
     *
     * @param a the first double value
     * @param b the second double value
     * @return -1 if a is greater than b, 0 if a is equal to be within 1e-6, 1 if b is greater than a.
     */
    public static byte compareDoubles(final double a, final double b) {
        return compareDoubles(a, b, 1e-6);
    }

    /**
     * Compares double values for equality (within epsilon), or inequality.
     *
     * @param a       the first double value
     * @param b       the second double value
     * @param epsilon the precision within which two double values will be considered equal
     * @return -1 if a is greater than b, 0 if a is equal to be within epsilon, 1 if b is greater than a.
     */
    public static byte compareDoubles(final double a, final double b, final double epsilon) {
        if (Math.abs(a - b) < epsilon) {
            return 0;
        }
        if (a > b) {
            return -1;
        }
        return 1;
    }

    /**
     */
    public static double log10BinomialCoefficient(final int n, final int k) {
        if ( n < 0 ) {
            throw new IllegalArgumentException("n: Must have non-negative number of trials");
        }
        if ( k > n || k < 0 ) {
            throw new IllegalArgumentException("k: Must have non-negative number of successes, and no more successes than number of trials");
        }

        return log10Factorial(n) - log10Factorial(k) - log10Factorial(n - k);
    }

    /**
     * binomial Probability(int, int, double) with log10 applied to result
     */
    public static double log10BinomialProbability(final int n, final int k, final double log10p) {
        if ( log10p > 1e-18 )
            throw new IllegalArgumentException("log10p: Log-probability must be 0 or less");
        double log10OneMinusP = Math.log10(1 - Math.pow(10, log10p));
        return log10BinomialCoefficient(n, k) + log10p * k + log10OneMinusP * (n - k);
    }

    public static double log10sumLog10(final double[] log10p, final int start) {
        return log10sumLog10(log10p, start, log10p.length);
    }

    public static double log10sumLog10(final double[] log10values) {
        return log10sumLog10(log10values, 0);
    }


    public static double log10sumLog10(final double[] log10p, final int start, final int finish) {

        if (start >= finish)
            return Double.NEGATIVE_INFINITY;
        final int maxElementIndex = MathUtils.maxElementIndex(log10p, start, finish);
        final double maxValue = log10p[maxElementIndex];
        if(maxValue == Double.NEGATIVE_INFINITY)
            return maxValue;
        double sum = 1.0;
        for (int i = start; i < finish; i++) {
            double curVal = log10p[i];
            double scaled_val = curVal - maxValue;
            if (i == maxElementIndex || curVal == Double.NEGATIVE_INFINITY) {
                continue;
            }
            else {
                sum += Math.pow(10.0, scaled_val);
            }
        }
        if ( Double.isNaN(sum) || sum == Double.POSITIVE_INFINITY ) {
            throw new IllegalArgumentException("log10p: Values must be non-infinite and non-NAN");
        }
        return maxValue + (sum != 1.0 ? Math.log10(sum) : 0.0);
    }

    /**
     * normalizes the log10-based array.  ASSUMES THAT ALL ARRAY ENTRIES ARE <= 0 (<= 1 IN REAL-SPACE).
     *
     * @param array             the array to be normalized
     * @param takeLog10OfOutput if true, the output will be transformed back into log10 units
     * @return a newly allocated array corresponding the normalized values in array, maybe log10 transformed
     */
    public static double[] normalizeFromLog10(final double[] array, final boolean takeLog10OfOutput) {
        return normalizeFromLog10(array, takeLog10OfOutput, false);
    }

    /**
     * See #normalizeFromLog10 but with the additional option to use an approximation that keeps the calculation always in log-space
     *
     * @param array
     * @param takeLog10OfOutput
     * @param keepInLogSpace
     *
     * @return
     */
    public static double[] normalizeFromLog10(final double[] array, final boolean takeLog10OfOutput, final boolean keepInLogSpace) {
        // for precision purposes, we need to add (or really subtract, since they're
        // all negative) the largest value; also, we need to convert to normal-space.
        double maxValue = arrayMax(array);

        // we may decide to just normalize in log space without converting to linear space
        if (keepInLogSpace) {
            for (int i = 0; i < array.length; i++) {
                array[i] -= maxValue;
            }
            return array;
        }

        // default case: go to linear space
        double[] normalized = new double[array.length];

        for (int i = 0; i < array.length; i++)
            normalized[i] = Math.pow(10, array[i] - maxValue);

        // normalize
        double sum = 0.0;
        for (int i = 0; i < array.length; i++)
            sum += normalized[i];
        for (int i = 0; i < array.length; i++) {
            double x = normalized[i] / sum;
            if (takeLog10OfOutput) {
                x = Math.log10(x);
                if ( x < LOG10_P_OF_ZERO || Double.isInfinite(x) )
                    x = array[i] - maxValue;
            }

            normalized[i] = x;
        }

        return normalized;
    }

    /**
     * normalizes the log10-based array.  ASSUMES THAT ALL ARRAY ENTRIES ARE <= 0 (<= 1 IN REAL-SPACE).
     *
     * @param array the array to be normalized
     * @return a newly allocated array corresponding the normalized values in array
     */
    public static double[] normalizeFromLog10(final double[] array) {
        return normalizeFromLog10(array, false);
    }

    /**
     * normalizes the real-space probability array.
     *
     * Does not assume anything about the values in the array, beyond that no elements are below 0.  It's ok
     * to have values in the array of > 1, or have the sum go above 0.
     *
     * @param array the array to be normalized
     * @return a newly allocated array corresponding the normalized values in array
     */
    public static double[] normalizeFromRealSpace(final double[] array) {
        if ( array.length == 0 )
            return array;

        final double sum = sum(array);
        final double[] normalized = new double[array.length];
        if ( sum < 0.0 ) throw new IllegalArgumentException("Values in probability array sum to a negative number " + sum);
        for ( int i = 0; i < array.length; i++ ) {
            normalized[i] = array[i] / sum;
        }
        return normalized;
    }

    public static int maxElementIndex(final double[] array) {
        return maxElementIndex(array, array.length);
    }

    public static int maxElementIndex(final double[] array, final int start, final int endIndex) {
        if (array == null || array.length == 0)
            throw new IllegalArgumentException("Array cannot be null!");

        if (start > endIndex) {
            throw new IllegalArgumentException("Start cannot be after end.");
        }

        int maxI = start;
        for (int i = (start+1); i < endIndex; i++) {
            if (array[i] > array[maxI])
                maxI = i;
        }
        return maxI;
    }

    public static int maxElementIndex(final double[] array, final int endIndex) {
        return maxElementIndex(array, 0, endIndex);
    }

    public static double arrayMax(final double[] array) {
        return array[maxElementIndex(array)];
    }

    /**
     * Checks that the result is a well-formed log10 probability
     *
     * @param result a supposedly well-formed log10 probability value.  By default allows
     *               -Infinity values, as log10(0.0) == -Infinity.
     * @return true if result is really well formed
     */
    public static boolean goodLog10Probability(final double result) {
        return goodLog10Probability(result, true);
    }

    /**
     * Checks that the result is a well-formed log10 probability
     *
     * @param result a supposedly well-formed log10 probability value
     * @param allowNegativeInfinity should we consider a -Infinity value ok?
     * @return true if result is really well formed
     */
    public static boolean goodLog10Probability(final double result, final boolean allowNegativeInfinity) {
        return result <= 0.0 && result != Double.POSITIVE_INFINITY && (allowNegativeInfinity || result != Double.NEGATIVE_INFINITY) && ! Double.isNaN(result);
    }

    /**
     * Checks that the result is a well-formed probability
     *
     * @param result a supposedly well-formed probability value
     * @return true if result is really well formed
     */
    public static boolean goodProbability(final double result) {
        return result >= 0.0 && result <= 1.0 && ! Double.isInfinite(result) && ! Double.isNaN(result);
    }

    //
    // useful common utility routines
    //

    /**
     * Converts LN to LOG10
     *
     * @param ln log(x)
     * @return log10(x)
     */
    public static double lnToLog10(final double ln) {
        return ln * LOG10_OF_E;
    }

    /**
     * Calculates the log10 of the gamma function for x.
     *
     * @param x the x parameter
     * @return the log10 of the gamma function at x.
     */
    public static double log10Gamma(final double x) {
        return lnToLog10(Gamma.logGamma(x));
    }


    public static double log10Factorial(final int x) {
        if (x >= Log10FactorialCache.size() || x < 0)
            return log10Gamma(x + 1);
        else
            return Log10FactorialCache.get(x);
    }

    /**
     * Wrapper class so that the log10Factorial array is only calculated if it's used
     */
    private static class Log10FactorialCache {

        /**
         * The size of the precomputed cache.  Must be a positive number!
         */
        private static final int CACHE_SIZE = 10_000;

        public static int size() { return CACHE_SIZE; }

        public static double get(final int n) {
            if (cache == null)
                initialize();
            return cache[n];
        }

        private static void initialize() {
            if (cache == null) {
                Log10Cache.ensureCacheContains(CACHE_SIZE);
                cache = new double[CACHE_SIZE];
                cache[0] = 0.0;
                for (int k = 1; k < cache.length; k++)
                    cache[k] = cache[k-1] + Log10Cache.get(k);
            }
        }

        private static double[] cache = null;
    }

    /**
     * Compute in a numerical correct way the quantity log10(1-x)
     *
     * Uses the approximation log10(1-x) = log10(1/x - 1) + log10(x) to avoid very quick underflow
     * in 1-x when x is very small
     *
     * @param x a positive double value between 0.0 and 1.0
     * @return an estimate of log10(1-x)
     */
    public static double log10OneMinusX(final double x) {
        if ( x == 1.0 )
            return Double.NEGATIVE_INFINITY;
        else if ( x == 0.0 )
            return 0.0;
        else {
            final double d = Math.log10(1 / x - 1) + Math.log10(x);
            return Double.isInfinite(d) || d > 0.0 ? 0.0 : d;
        }
    }

    /**
     * Now for some matrix methods
     */

    /**
     *
     * @param m a real-valued matrix
     * @return whether m is symmetric
     */
    public static boolean isSymmetric(RealMatrix m) {
        return m.equals(m.transpose());
    }

    /**
     *
     * @param m a real-valued matrix
     * @return whether m is positive semi-definite i.e. has no negative eigenvalues
     */
    public static boolean isPositiveSemiDefinite(RealMatrix m) {
        EigenDecomposition ed = new EigenDecomposition(m);
        for (final double eigval : ed.getRealEigenvalues()) {
            if (eigval < 0) return false;
        }
        return true;
    }

    /**
     * Compute the logarithm of a square matrix.  Unfortunately, Aoache Commons does not have this method.
     *
     * We compute the matrix logarithm by diagonalizing, taking logarithms of the diagonal entries, and
     * reversing the diagonalizing change of basis
     *
     * @param M
     * @return the matrix logarithm of M
     */
    public static RealMatrix matrixLog(RealMatrix M) {
        EigenDecomposition ed = new EigenDecomposition(M);
        RealMatrix D = ed.getD();   //D is diagonal
        RealMatrix V = ed.getV();   //M = V*D*V^T; V is the diagonalizing change of basis

        //replace D (in-place) by its logarithm
        for (int i = 0; i < M.getColumnDimension(); i++) {
            D.setEntry(i, i, Math.log(D.getEntry(i, i)));
        }

        return V.multiply(D).multiply(V.transpose());   //reverse the change of basis
    }

    /**
     * Measure the difference between two covariance matrices in terms of the Kullback-Leibler
     * divergence between associated Gaussians.
     *
     * If d is the dimension of these matrices, the KL divergence between zero-centered Gaussians
     * with covariances A and B is (1/2){tr[A^(-1)B] + ln(det(A) - ln(det(B)) - d}.  Note: the KL
     * divergence is not symmetric.  Switching A <--> B and averaging gives (1/2){tr[A^(-1)B] + tr[B^(-1)A] - d}
     *
     * @param cov1 a matrix covariance
     * @param cov2 a matrix covariance
     * @return the average of KL divergences, (KL(p|q) + KL(q|p))/2, where p and q are probability densities
     * of zero-centered Gaussians with the give covariance
     */
    public static double covarianceKLDivergence(RealMatrix cov1, RealMatrix cov2) {
        if (!isSymmetric(cov1) || !isSymmetric(cov2)) {
            throw new GATKException("Covariance matrices must be symmetric.");
        }

        if (!isPositiveSemiDefinite(cov1) || !isPositiveSemiDefinite(cov2)) {
            throw new GATKException("Covariance matrices must be positive semidefinite.");
        }

        int d = cov1.getRowDimension();

        if (cov1.getRowDimension() != cov2.getRowDimension()) {
            throw new GATKException("Can only compare covariance matrices of equal dimension.");
        }

        LUDecomposition LU1 = new LUDecomposition(cov1);
        LUDecomposition LU2 = new LUDecomposition(cov2);

        return (LU1.getSolver().solve(cov2).getTrace() + LU2.getSolver().solve(cov1).getTrace() - d)/2;
    }

    /**
     * Measure the geodesic distance between the two covariances within the manifold of symmetric,
     * positive-definite matrices.  This is also called the affine-invariant metric.
     *
     * The formula is ||log(A^(-1/2)*B*A^(-1/2)||_F, where ||    ||_F is the Frobenius norm.  This formula
     * is symmetric despite its appearance.
     *
     * For positive semidefinite matrices with eigendecomposition M = V*D*V^(-1), where D is diagonal
     * the matrix inverse square root is M^(-1/2) = V*D^(-1/2)*V^(-1)
     *
     * @param cov1 a covariance matrix
     * @param cov2 a covariance matrix
     * @return the geodesic distance between cov1 and cov2 in the manifold of positive semi-definite
     * symmetric matrices, which is more natural than the Euclidean distance inherited from the embedding
     * in R^(d^2)
     */
    public static double covarianceGeodesicDistance(RealMatrix cov1, RealMatrix cov2) {
        if (!isSymmetric(cov1) || !isSymmetric(cov2)) {
            throw new GATKException("Covariance matrices must be symmetric.");
        }

        if (!isPositiveSemiDefinite(cov1) || !isPositiveSemiDefinite(cov2)) {
            throw new GATKException("Covariance matrices must be positive semidefinite.");
        }

        if (cov1.getRowDimension() != cov2.getRowDimension()) {
            throw new GATKException("Can only compare covariance matrices of equal dimension.");
        }

        RealMatrix sqrt = (new EigenDecomposition(cov1)).getSquareRoot();
        RealMatrix inverseSqrt = (new LUDecomposition(sqrt)).getSolver().getInverse();

        //the thing inside the matrix logarithm
        RealMatrix mat = inverseSqrt.multiply(cov2).multiply(inverseSqrt);
        return matrixLog(mat).getFrobeniusNorm();

    }

    /** Calculate the mean of an array of doubles. */
    public static double mean(final double[] in, final int start, final int stop) {
        if ((stop - start) <= 0 ) return Double.NaN;

        double total = 0;
        for (int i = start; i < stop; ++i) {
            total += in[i];
        }

        return total / (stop - start);
    }

    /** Calculate the (population) standard deviation of an array of doubles. */
    public static double stddev(final double[] in, final int start, final int length) {
        return stddev(in, start, length, mean(in, start, length));
    }

    /** Calculate the (population) standard deviation of an array of doubles. */
    public static double stddev(final double[] in, final int start, final int stop, final double mean) {
        if ((stop - start) <= 0) return Double.NaN;

        double total = 0;
        for (int i = start; i < stop; ++i) {
            total += (in[i] * in[i]);
        }

        return Math.sqrt((total / (stop - start)) - (mean * mean));
    }

    /** "Promotes" an int[] into a double array with the same values (or as close as precision allows). */
    public static double[] promote(final int[] is) {
        final double[] ds = new double[is.length];
        for (int i = 0; i < is.length; ++i) ds[i] = is[i];
        return ds;
    }
}
