/**
 * HeroScene — Framer Motion, shape-based visual
 * Drop into: frontend-client/src/components/HeroScene.tsx
 *
 * In LandingPage.tsx replace:
 *   const HeroScene = lazy(() => ...)
 * with:
 *   import { HeroScene } from "@/components/HeroScene";
 * and remove the <Suspense> wrapper.
 *
 * Install: bun add framer-motion
 */
import { motion } from "framer-motion";
import { MapPin, TrendingUp, Shield, Building2 } from "lucide-react";

// ── Floating data pill ──────────────────────────────────────────────────────
function DataPill({
  icon: Icon,
  label,
  value,
  color,
  delay,
  className,
}: {
  icon: React.ElementType;
  label: string;
  value: string;
  color: string;
  delay: number;
  className?: string;
}) {
  return (
    <motion.div
      className={`absolute flex items-center gap-2 bg-white/90 dark:bg-card/90 backdrop-blur-sm border border-border/60 rounded-full px-3 py-1.5 shadow-lg shadow-black/5 ${className}`}
      initial={{ opacity: 0, scale: 0.7, y: 10 }}
      animate={{ opacity: 1, scale: 1, y: 0 }}
      transition={{ duration: 0.5, delay, ease: [0.22, 1, 0.36, 1] }}
    >
      <motion.div
        className={`h-6 w-6 rounded-full flex items-center justify-center shrink-0 ${color}`}
        animate={{ scale: [1, 1.12, 1] }}
        transition={{ duration: 2.4, repeat: Infinity, delay: delay + 0.5 }}
      >
        <Icon className="h-3 w-3" />
      </motion.div>
      <div className="leading-none">
        <div className="text-[10px] text-muted-foreground">{label}</div>
        <div className="text-xs font-bold text-foreground">{value}</div>
      </div>
    </motion.div>
  );
}

// ── Main component ──────────────────────────────────────────────────────────
export function HeroScene() {
  return (
    <div className="relative w-full h-[400px] md:h-[500px] overflow-hidden">

      {/* ── Background glow blobs ───────────────────────────────────────── */}
      <motion.div
        className="absolute inset-0 pointer-events-none"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 1 }}
      >
        <div className="absolute top-8 right-12 w-56 h-56 rounded-full bg-primary/10 blur-3xl" />
        <div className="absolute bottom-12 left-8 w-40 h-40 rounded-full bg-[hsl(200_70%_50%)]/10 blur-3xl" />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-72 h-72 rounded-full bg-primary/5 blur-3xl" />
      </motion.div>

      {/* ── SVG scene ───────────────────────────────────────────────────── */}
      <svg
        viewBox="0 0 480 420"
        className="absolute inset-0 w-full h-full"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        {/* Ground shadow */}
        <motion.ellipse
          cx="240" cy="390" rx="170" ry="14"
          fill="hsl(231 72% 60%)"
          opacity={0}
          animate={{ opacity: 0.06 }}
          transition={{ duration: 0.8, delay: 0.2 }}
        />

        {/* ── Back-left building ── */}
        <motion.g
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, delay: 0.1, ease: [0.22, 1, 0.36, 1] }}
        >
          {/* Body */}
          <rect x="58" y="210" width="72" height="178" rx="4" fill="hsl(231 72% 60%)" opacity="0.13" />
          {/* Facade grid */}
          {[0,1,2,3,4,5].map(row =>
            [0,1,2].map(col => (
              <motion.rect
                key={`bl-${row}-${col}`}
                x={68 + col * 20} y={220 + row * 26} width="12" height="16" rx="2"
                fill="hsl(231 72% 60%)"
                opacity={0}
                animate={{ opacity: 0.35 }}
                transition={{ duration: 0.3, delay: 0.5 + row * 0.06 + col * 0.04 }}
              />
            ))
          )}
          {/* Roof accent */}
          <rect x="62" y="204" width="72" height="8" rx="2" fill="hsl(231 72% 60%)" opacity="0.22" />
        </motion.g>

        {/* ── Tall center building ── */}
        <motion.g
          initial={{ opacity: 0, y: 50 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.8, delay: 0.25, ease: [0.22, 1, 0.36, 1] }}
        >
          {/* Body */}
          <rect x="168" y="118" width="100" height="270" rx="6" fill="hsl(231 72% 60%)" opacity="0.18" />
          {/* Gradient overlay */}
          <rect x="168" y="118" width="100" height="270" rx="6"
            fill="url(#centerGrad)" opacity="0.7" />
          {/* Windows grid */}
          {[0,1,2,3,4,5,6,7].map(row =>
            [0,1,2,3].map(col => (
              <motion.rect
                key={`ct-${row}-${col}`}
                x={178 + col * 22} y={130 + row * 30} width="14" height="18" rx="2"
                fill="hsl(231 72% 60%)"
                opacity={0}
                animate={{ opacity: 0.3 + (col % 2) * 0.15 }}
                transition={{ duration: 0.3, delay: 0.6 + row * 0.05 + col * 0.04 }}
              />
            ))
          )}
          {/* Roof */}
          <rect x="164" y="110" width="108" height="10" rx="3" fill="hsl(231 72% 60%)" opacity="0.3" />
          {/* Antenna */}
          <motion.rect
            x="215" y="80" width="4" height="32" rx="2"
            fill="hsl(231 72% 60%)" opacity="0.4"
            initial={{ scaleY: 0, originY: 1 }}
            animate={{ scaleY: 1 }}
            transition={{ duration: 0.4, delay: 0.9 }}
          />
          <motion.circle
            cx="217" cy="78" r="4"
            fill="hsl(231 72% 60%)" opacity="0.7"
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ duration: 0.3, delay: 1.1, type: "spring" }}
          />
        </motion.g>

        {/* ── Right building ── */}
        <motion.g
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, delay: 0.35, ease: [0.22, 1, 0.36, 1] }}
        >
          <rect x="298" y="168" width="82" height="220" rx="5" fill="hsl(200 70% 50%)" opacity="0.14" />
          {[0,1,2,3,4,5].map(row =>
            [0,1,2].map(col => (
              <motion.rect
                key={`rt-${row}-${col}`}
                x={308 + col * 22} y={180 + row * 32} width="14" height="20" rx="2"
                fill="hsl(200 70% 50%)"
                opacity={0}
                animate={{ opacity: 0.32 }}
                transition={{ duration: 0.3, delay: 0.65 + row * 0.06 + col * 0.04 }}
              />
            ))
          )}
          <rect x="294" y="162" width="90" height="8" rx="2" fill="hsl(200 70% 50%)" opacity="0.22" />
        </motion.g>

        {/* ── Far-right small building ── */}
        <motion.g
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.45, ease: [0.22, 1, 0.36, 1] }}
        >
          <rect x="394" y="248" width="52" height="140" rx="4" fill="hsl(280 60% 50%)" opacity="0.11" />
          {[0,1,2].map(row =>
            [0,1].map(col => (
              <motion.rect
                key={`fr-${row}-${col}`}
                x={402 + col * 20} y={258 + row * 36} width="12" height="22" rx="2"
                fill="hsl(280 60% 50%)"
                opacity={0}
                animate={{ opacity: 0.28 }}
                transition={{ duration: 0.3, delay: 0.7 + row * 0.07 }}
              />
            ))
          )}
        </motion.g>

        {/* ── Ground / street line ── */}
        <motion.rect
          x="40" y="387" width="400" height="3" rx="1.5"
          fill="hsl(231 72% 60%)" opacity={0}
          animate={{ opacity: 0.12 }}
          transition={{ duration: 0.5, delay: 0.8 }}
        />

        {/* ── Floating abstract shape — large ring ── */}
        <motion.circle
          cx="380" cy="110" r="44"
          stroke="hsl(231 72% 60%)" strokeWidth="2" opacity="0"
          strokeDasharray="12 8"
          animate={{ opacity: 0.22, rotate: 360 }}
          transition={{ opacity: { duration: 0.5, delay: 0.9 }, rotate: { duration: 22, repeat: Infinity, ease: "linear" } }}
          style={{ transformOrigin: "380px 110px" }}
        />

        {/* ── Small orbit dot ── */}
        <motion.circle
          cx="424" cy="110" r="5"
          fill="hsl(231 72% 60%)" opacity={0}
          animate={{ opacity: 0.6 }}
          transition={{ duration: 0.3, delay: 1.1 }}
        />

        {/* ── Bottom-left decorative hexagon ── */}
        <motion.polygon
          points="76,342 98,330 120,342 120,366 98,378 76,366"
          stroke="hsl(200 70% 50%)" strokeWidth="1.5"
          fill="hsl(200 70% 50%)" opacity={0}
          animate={{ opacity: 0.08 }}
          transition={{ duration: 0.5, delay: 1.0 }}
        />
        <motion.polygon
          points="76,342 98,330 120,342 120,366 98,378 76,366"
          stroke="hsl(200 70% 50%)" strokeWidth="1.5"
          fill="none"
          opacity={0}
          animate={{ opacity: 0.2 }}
          transition={{ duration: 0.5, delay: 1.0 }}
        />

        {/* ── Scattered dots ── */}
        {[
          { cx: 50,  cy: 160, r: 3, c: "hsl(231 72% 60%)", o: 0.25, d: 0.8 },
          { cx: 440, cy: 220, r: 2.5, c: "hsl(200 70% 50%)", o: 0.2, d: 0.9 },
          { cx: 150, cy: 380, r: 3.5, c: "hsl(231 72% 60%)", o: 0.18, d: 1.0 },
          { cx: 350, cy: 370, r: 2.5, c: "hsl(280 60% 50%)", o: 0.22, d: 1.05 },
          { cx: 30,  cy: 300, r: 2,   c: "hsl(231 72% 60%)", o: 0.15, d: 1.1 },
        ].map((dot, i) => (
          <motion.circle
            key={i}
            cx={dot.cx} cy={dot.cy} r={dot.r}
            fill={dot.c}
            opacity={0}
            animate={{ opacity: dot.o }}
            transition={{ duration: 0.4, delay: dot.d }}
          />
        ))}

        {/* Gradient def */}
        <defs>
          <linearGradient id="centerGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="hsl(231 72% 60%)" stopOpacity="0.08" />
            <stop offset="100%" stopColor="hsl(231 72% 60%)" stopOpacity="0.22" />
          </linearGradient>
        </defs>
      </svg>

      {/* ── Floating data pills ─────────────────────────────────────────── */}
      <DataPill
        icon={Building2}
        label="Annonces actives"
        value="12 400+"
        color="bg-primary/10 text-primary"
        delay={0.8}
        className="top-[14%] left-[4%]"
      />
      <DataPill
        icon={MapPin}
        label="Région"
        value="Grand Tunis"
        color="bg-emerald-500/10 text-emerald-600"
        delay={0.95}
        className="top-[8%] right-[8%]"
      />
      <DataPill
        icon={TrendingUp}
        label="Prix moyen / m²"
        value="2 850 DT"
        color="bg-amber-500/10 text-amber-600"
        delay={1.05}
        className="bottom-[22%] left-[6%]"
      />
      <DataPill
        icon={Shield}
        label="Fraude détectée"
        value="98.2% propre"
        color="bg-violet-500/10 text-violet-600"
        delay={1.15}
        className="bottom-[18%] right-[5%]"
      />

      {/* ── Continuous float animation wrapper ─────────────────────────── */}
      <motion.div
        className="absolute inset-0 pointer-events-none"
        animate={{ y: [0, -6, 0] }}
        transition={{ duration: 5, repeat: Infinity, ease: "easeInOut" }}
      />
    </div>
  );
}