import Refpoly.CudaIP

/-!
# Certified GPU acceptance

`device_ip_acceptance_verified` (src/gpu/ip_check.cuh) accepts only with five
points `B`, an integer matrix `A` and `d ≠ 0` with `A * B = d • 1`, and points
`P_r` with multiplicities `μ_r ≥ 0` (not all zero) and `k > 0` such that
`∑ μ_r P_r + k ∑ B_r = 0`. These facts alone prove the origin interior: the
hull contains the independent `B_r` and `-c ∑ B_r` for some `c > 0`. How the
hull construction or the certificate search behaved is irrelevant.
-/
namespace Refpoly.CudaIP
open Set


/-- Five independent hull points and a positive multiple of minus their sum
in the hull put the origin in the interior. -/
theorem positive_spanning_ip {d : ℕ} (S : Set (Fin d → ℝ)) (b : Fin d → Fin d → ℝ)
    (hb : LinearIndependent ℝ b) (hbS : ∀ i, b i ∈ convexHull ℝ S)
    (q : Fin d → ℝ) (hq : q ∈ convexHull ℝ S) (c : ℝ) (hc : 0 < c)
    (hqeq : q = -(c • ∑ i, b i)) : (0 : Fin d → ℝ) ∈ interior (convexHull ℝ S) := by
  classical
  let L := Fintype.linearCombination ℝ b
  have hinj : Function.Injective L := linearIndependent_iff_injective_fintypeLinearCombination.mp hb
  have hsurj : Function.Surjective L := LinearMap.injective_iff_surjective.mp hinj
  let e := (LinearEquiv.ofBijective L ⟨hinj, hsurj⟩).toContinuousLinearEquiv
  have he : ∀ t, e t = ∑ i, t i • b i := fun t => by
    simp [e, L, Fintype.linearCombination_apply]
  set D : ℝ := 1 + d * c with hD
  have hDpos : 0 < D := by positivity
  set δ : ℝ := c / (4 * D) with hδ
  have hδpos : 0 < δ := by positivity
  have hopen : IsOpen (e '' Metric.ball 0 δ) := e.isOpenMap _ Metric.isOpen_ball
  have h0 : (0 : Fin d → ℝ) ∈ e '' Metric.ball 0 δ := ⟨0, Metric.mem_ball_self hδpos, map_zero e⟩
  have hsub : e '' Metric.ball 0 δ ⊆ convexHull ℝ S := by
    rintro _ ⟨t, ht, rfl⟩
    have hti : ∀ i, |t i| < δ := by
      intro i
      have h := (pi_norm_lt_iff hδpos).mp (mem_ball_zero_iff.mp ht) i
      simpa [Real.norm_eq_abs] using h
    set s := ∑ i, t i with hs
    have hsabs : |s| ≤ d * δ := by
      calc |s| ≤ ∑ i, |t i| := Finset.abs_sum_le_sum_abs _ _
        _ ≤ ∑ _i : Fin d, δ := Finset.sum_le_sum (fun i _ => (hti i).le)
        _ = d * δ := by simp
    have hdδ : d * δ ≤ 1 / 4 := by
      rw [hδ, mul_div_assoc']
      rw [div_le_div_iff₀ (by positivity) (by norm_num)]
      nlinarith
    set l0 : ℝ := (1 - s) / D with hl0
    have hl0pos : 3 / (4 * D) ≤ l0 := by
      rw [hl0, div_le_div_iff₀ (by positivity) hDpos]
      have : s ≤ 1/4 := le_trans (le_abs_self s) (hsabs.trans hdδ)
      nlinarith
    let w : Option (Fin d) → ℝ := fun o => o.elim l0 (fun i => t i + c * l0)
    let z : Option (Fin d) → Fin d → ℝ := fun o => o.elim q b
    have hw : ∀ o ∈ (Finset.univ : Finset (Option (Fin d))), 0 ≤ w o := by
      intro o _
      cases o with
      | none => simp only [w, Option.elim]; linarith [show (0:ℝ) < 3 / (4 * D) by positivity]
      | some i =>
        simp only [w, Option.elim]
        have h1 := neg_abs_le (t i)
        have h2 := hti i
        have h3 : c * (3 / (4 * D)) ≤ c * l0 := mul_le_mul_of_nonneg_left hl0pos hc.le
        have h4 : c * (3 / (4 * D)) = 3 * δ := by rw [hδ]; ring
        linarith
    have hsum : ∑ o, w o = 1 := by
      rw [Fintype.sum_option]
      simp only [w, Option.elim, Finset.sum_add_distrib, ← hs, Finset.sum_const, Finset.card_univ,
        Fintype.card_fin, nsmul_eq_mul]
      rw [hl0]
      field_simp
      ring
    have hz : ∀ o ∈ (Finset.univ : Finset (Option (Fin d))), z o ∈ convexHull ℝ S := by
      intro o _
      cases o with
      | none => exact hq
      | some i => exact hbS i
    have hmem := (convex_convexHull ℝ S).sum_mem hw hsum hz
    have heq : ∑ o, w o • z o = e t := by
      rw [Fintype.sum_option, he]
      simp only [w, z, Option.elim, hqeq, add_smul, Finset.sum_add_distrib, smul_neg, Finset.smul_sum,
        smul_smul]
      have : ∑ x, (c * l0) • b x = ∑ x, (l0 * c) • b x := by
        refine Finset.sum_congr rfl (fun x _ => by rw [mul_comm])
      rw [this]
      abel
    rw [← heq]
    exact hmem
  exact interior_mono hsub (by rwa [hopen.interior_eq])

/-- Integer form with a nonzero determinant. -/
theorem integer_positive_spanning_ip {d m : ℕ} (xs : List (Refpoly.KS.IntPoint d))
    (B : Fin d → Refpoly.KS.IntPoint d) (hB : ∀ r, B r ∈ xs) (hdet : (Matrix.of B).det ≠ 0)
    (pts : Fin m → Refpoly.KS.IntPoint d) (hpts : ∀ i, pts i ∈ xs) (μ : Fin m → ℕ)
    (k : ℕ) (hk : 0 < k) (hM : 0 < ∑ i, μ i)
    (hzero : ∑ i, (μ i : ℤ) • pts i + (k : ℤ) • ∑ r, B r = 0) :
    Refpoly.IsIP (Refpoly.KS.PolyCode.hull ⟨xs⟩) := by
  classical
  set S := Refpoly.KS.pointSet xs
  have mem : ∀ x ∈ xs, Refpoly.KS.ofInt x ∈ convexHull ℝ S :=
    fun x hx => subset_convexHull ℝ S ⟨x, hx, rfl⟩
  let b : Fin d → Fin d → ℝ := fun r => Refpoly.KS.ofInt (B r)
  have hb : LinearIndependent ℝ b := by
    have hdetR : ((Matrix.of B).map (Int.castRingHom ℝ)).det ≠ 0 := by
      rw [← RingHom.mapMatrix_apply, ← RingHom.map_det]
      exact Int.cast_ne_zero.mpr hdet
    have := Matrix.linearIndependent_rows_of_det_ne_zero hdetR
    convert this using 1
  set M : ℝ := ∑ i, (μ i : ℝ) with hMdef
  have hMpos : 0 < M := by rw [hMdef]; exact_mod_cast hM
  let q : Fin d → ℝ := ∑ i, ((μ i : ℝ) / M) • Refpoly.KS.ofInt (pts i)
  have hq : q ∈ convexHull ℝ S := by
    apply (convex_convexHull ℝ S).sum_mem
    · intro i _; positivity
    · rw [← Finset.sum_div, ← hMdef, div_self hMpos.ne']
    · intro i _; exact mem _ (hpts i)
  have hreal : ∑ i, (μ i : ℝ) • Refpoly.KS.ofInt (pts i) + (k : ℝ) • ∑ r, b r = 0 := by
    have h := congrArg Refpoly.KS.ofInt hzero
    funext j
    have hj := congrFun h j
    simp only [Refpoly.KS.ofInt, Finset.sum_apply, Pi.add_apply, Pi.smul_apply, smul_eq_mul,
      Pi.zero_apply, Int.cast_add, Int.cast_sum, Int.cast_mul, Int.cast_natCast, Int.cast_zero] at hj ⊢
    simpa [b, Refpoly.KS.ofInt, Finset.sum_apply] using hj
  have hqeq : q = -(((k : ℝ) / M) • ∑ r, b r) := by
    have : q = (1 / M) • ∑ i, (μ i : ℝ) • Refpoly.KS.ofInt (pts i) := by
      simp only [q, Finset.smul_sum, smul_smul]
      refine Finset.sum_congr rfl (fun i _ => by ring_nf)
    rw [this, eq_neg_of_add_eq_zero_left hreal, smul_neg, smul_smul]
    ring_nf
  exact positive_spanning_ip S b hb (fun r => mem _ (hB r)) q hq ((k : ℝ) / M)
    (by have : (0:ℝ) < k := by exact_mod_cast hk
        positivity) hqeq


/-- An adjugate-style witness `A * B = d • 1` with `d ≠ 0` proves `det B ≠ 0`
without trusting any determinant computation. -/
theorem det_ne_zero_of_witness {n : ℕ} (A M : Matrix (Fin n) (Fin n) ℤ) (d : ℤ) (hd : d ≠ 0)
    (h : A * M = d • (1 : Matrix (Fin n) (Fin n) ℤ)) : M.det ≠ 0 := by
  intro hz
  have := congrArg Matrix.det h
  rw [Matrix.det_mul, hz, mul_zero, Matrix.det_smul, Matrix.det_one, mul_one] at this
  exact pow_ne_zero _ hd this.symm

/-- Exactly the conditions `device_ip_acceptance_verified` checks. -/
theorem checked_acceptance (xs : List (KS.IntPoint 5)) (B : Fin 5 → KS.IntPoint 5)
    (hB : ∀ r, B r ∈ xs) (A : Matrix (Fin 5) (Fin 5) ℤ) (d : ℤ) (hd : d ≠ 0)
    (hA : A * Matrix.of B = d • (1 : Matrix (Fin 5) (Fin 5) ℤ))
    (P : Fin 5 → KS.IntPoint 5) (hP : ∀ r, P r ∈ xs) (μ : Fin 5 → ℕ) (hμ : 0 < ∑ r, μ r)
    (k : ℕ) (hk : 0 < k) (hzero : ∑ r, (μ r : ℤ) • P r + (k : ℤ) • ∑ r, B r = 0) :
    IsIP (KS.PolyCode.hull ⟨xs⟩) :=
  integer_positive_spanning_ip xs B hB (det_ne_zero_of_witness A _ d hd hA) P hP μ k hk hμ hzero

end Refpoly.CudaIP
