from scipy.stats._stats_py import _attempt_exact_2kssamp
from scipy.stats import distributions
from math import gcd
import warnings
import numpy as np

def ks_2sampCDF(cdf1, cdf2, alternative='two-sided', method='auto'):
    mode = method

    if mode not in ['auto', 'exact', 'asymp']:
        raise ValueError(f'Invalid value for mode: {mode}')
    alternative = {'t': 'two-sided', 'g': 'greater', 'l': 'less'}.get(
        alternative.lower()[0], alternative)
    if alternative not in ['two-sided', 'less', 'greater']:
        raise ValueError(f'Invalid value for alternative: {alternative}')
    MAX_AUTO_N = 10000  # 'auto' will attempt to be exact if n1,n2 <= MAX_AUTO_N

    n1 = cdf1.shape[0]
    n2 = cdf2.shape[0]
    if min(n1, n2) == 0:
        raise ValueError('Data passed to ks_2samp must not be empty')

    # using searchsorted solves equal data problem
    cddiffs = cdf1 - cdf2

    # Identify the location of the statistic
    argminS = np.argmin(cddiffs)
    argmaxS = np.argmax(cddiffs)


    # Ensure sign of minS is not negative.
    minS = np.clip(-cddiffs[argminS], 0, 1)
    maxS = cddiffs[argmaxS]

    if alternative == 'less' or (alternative == 'two-sided' and minS > maxS):
        d = minS
        d_sign = -1
    else:
        d = maxS
        d_sign = 1
    g = gcd(n1, n2)
    n1g = n1 // g
    n2g = n2 // g
    prob = -np.inf
    if mode == 'auto':
        mode = 'exact' if max(n1, n2) <= MAX_AUTO_N else 'asymp'
    elif mode == 'exact':
        # If lcm(n1, n2) is too big, switch from exact to asymp
        if n1g >= np.iinfo(np.int32).max / n2g:
            mode = 'asymp'
            warnings.warn(
                f"Exact ks_2samp calculation not possible with samples sizes "
                f"{n1} and {n2}. Switching to 'asymp'.", RuntimeWarning,
                stacklevel=3)

    if mode == 'exact':
        success, d, prob = _attempt_exact_2kssamp(n1, n2, g, d, alternative)
        if not success:
            mode = 'asymp'
            warnings.warn(f"ks_2samp: Exact calculation unsuccessful. "
                          f"Switching to method={mode}.", RuntimeWarning,
                          stacklevel=3)

    if mode == 'asymp':
        # The product n1*n2 is large.  Use Smirnov's asymptoptic formula.
        # Ensure float to avoid overflow in multiplication
        # sorted because the one-sided formula is not symmetric in n1, n2
        m, n = sorted([float(n1), float(n2)], reverse=True)
        en = m * n / (m + n)
        if alternative == 'two-sided':
            prob = distributions.kstwo.sf(d, np.round(en))
        else:
            z = np.sqrt(en) * d
            # Use Hodges' suggested approximation Eqn 5.3
            # Requires m to be the larger of (n1, n2)
            expt = -2 * z**2 - 2 * z * (m + 2*n)/np.sqrt(m*n*(m+n))/3.0
            prob = np.exp(expt)

    prob = np.clip(prob, 0, 1)
    return {'ks-statistic': d, 'p-value': prob, 'sign': d_sign}