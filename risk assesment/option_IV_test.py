#!/usr/bin/env python3
"""
implied_vol.py  –  Black-Scholes implied-volatility solver
  * Newton–Raphson with Vega slope
  * Brent-bisection fallback
  * Includes dividend yield q (defaults to 0.0)
"""

import math

# ---------- standard-normal helpers ----------
SQRT_2PI = math.sqrt(2.0 * math.pi)
phi = lambda x: math.exp(-0.5 * x * x) / SQRT_2PI      # PDF
Phi = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))  # CDF


# ---------- Black-Scholes price ----------
def bs_price(S, K, r, q, T, sigma, cp):
    """Call (+1) or put (−1) price with continuous q."""
    if sigma <= 0.0 or T <= 0.0:
        return max(cp * (S - K), 0.0)
    sqT = math.sqrt(T)
    d1  = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
    d2  = d1 - sigma * sqT
    disc_q, disc_r = math.exp(-q * T), math.exp(-r * T)
    if cp == +1:   # call
        return S * disc_q * Phi(d1) - K * disc_r * Phi(d2)
    else:          # put
        return K * disc_r * Phi(-d2) - S * disc_q * Phi(-d1)


# ---------- Vega ----------
def bs_vega(S, r, q, T, d1):
    return S * math.exp(-q * T) * phi(d1) * math.sqrt(T)


# ---------- Implied-vol solver ----------
def implied_vol(S, K, r, T, P_obs, cp, q=0.0,
                tol_price=1e-6, tol_sigma=1e-7, max_iter=100):
    sigma = 0.20                               # initial guess 20 %
    for _ in range(max_iter):                  # Newton loop
        sqT = math.sqrt(T)
        d1  = (math.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * sqT)
        price = bs_price(S, K, r, q, T, sigma, cp)
        diff  = price - P_obs                  # f(σ)
        if abs(diff) < tol_price:
            return sigma
        vega = bs_vega(S, r, q, T, d1)         # f′(σ)
        if vega < 1e-12:
            break                              # fallback if slope ~ 0
        step  = diff / vega
        sigma -= step
        if abs(step) < tol_sigma:
            return sigma

    # Brent fallback (guaranteed root in [1e-6, 5])
    low, high = 1e-6, 5.0
    f_low  = bs_price(S, K, r, q, T, low,  cp) - P_obs
    f_high = bs_price(S, K, r, q, T, high, cp) - P_obs
    if f_low * f_high >= 0:
        raise RuntimeError("Root not bracketed; check inputs.")
    for _ in range(200):
        mid = 0.5 * (low + high)
        f_mid = bs_price(S, K, r, q, T, mid, cp) - P_obs
        if abs(f_mid) < tol_price:
            return mid
        if f_low * f_mid < 0:
            high, f_high = mid, f_mid
        else:
            low,  f_low  = mid, f_mid
    raise RuntimeError("Brent method failed to converge.")


# ---------- quick demo ----------
if __name__ == "__main__":
    S, K   = 100.0, 105.0
    r      = 0.0436          # 4.36 % continuous
    T      = 35 / 365        # 35-day option
    q      = 0.0             # dividend yield (change if needed)
    P_mkt  = 3.42            # observed call price
    cp     = +1              # +1 call, −1 put

    iv = implied_vol(S, K, r, T, P_mkt, cp, q=q)
    print(f"Implied volatility = {iv:.4%}")
 