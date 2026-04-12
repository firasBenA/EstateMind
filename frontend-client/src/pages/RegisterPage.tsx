/**
 * frontend-client/src/pages/RegisterPage.tsx
 *
 * Registration form with:
 *  - Real API via AuthContext
 *  - Client-side validation mirroring backend rules
 *  - Date of birth + age ≥ 18 check
 *  - Password strength (8 chars, 1 uppercase, 1 digit, 1 special)
 *  - Agency: agency_name + matricule_fiscale (format 1234567A/A/AAA/000)
 */
import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useAuth } from "@/lib/auth-context";
import { PublicLayout } from "@/components/PublicLayout";
import { Button }   from "@/components/ui/button";
import { Input }    from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Label }    from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { toast }    from "sonner";
import { User, Building2, Eye, EyeOff, CheckCircle2, XCircle, AlertCircle } from "lucide-react";

// ── Validation ────────────────────────────────────────────────────────────────

const MATRICULE_RE = /^\d{7}[A-Z]\/[A-Z]\/[A-Z]{3}\/\d{3}$/;

function calcAge(dob: string): number {
  if (!dob) return 0;
  const d     = new Date(dob);
  const today = new Date();
  let age = today.getFullYear() - d.getFullYear();
  const m = today.getMonth() - d.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < d.getDate())) age--;
  return age;
}

function validateClient(fields: {
  name: string; email: string; password: string; confirmPassword: string;
  dob: string; role: "particular" | "agency";
  agencyName: string; matricule: string;
}): Record<string, string> {
  const e: Record<string, string> = {};

  if (fields.name.trim().length < 2)
    e.name = "Le nom doit contenir au moins 2 caractères.";

  if (!/^[^@]+@[^@]+\.[^@]+$/.test(fields.email))
    e.email = "Adresse email invalide.";

  if (fields.password.length < 8)
    e.password = "Au moins 8 caractères.";
  else if (!/[A-Z]/.test(fields.password))
    e.password = "Au moins une lettre majuscule.";
  else if (!/[0-9]/.test(fields.password))
    e.password = "Au moins un chiffre.";
  else if (!/[^A-Za-z0-9]/.test(fields.password))
    e.password = "Au moins un caractère spécial (@, #, !, …).";

  if (fields.confirmPassword !== fields.password)
    e.confirmPassword = "Les mots de passe ne correspondent pas.";

  if (!fields.dob)
    e.dob = "La date de naissance est obligatoire.";
  else if (new Date(fields.dob) > new Date())
    e.dob = "La date ne peut pas être dans le futur.";
  else if (calcAge(fields.dob) < 18)
    e.dob = "Vous devez avoir au moins 18 ans pour vous inscrire.";

  if (fields.role === "agency") {
    if (fields.agencyName.trim().length < 2)
      e.agencyName = "Le nom de l'agence est obligatoire.";
    const mf = fields.matricule.trim().toUpperCase();
    if (!mf)
      e.matricule = "Le matricule fiscal est obligatoire pour les agences.";
    else if (!MATRICULE_RE.test(mf))
      e.matricule = "Format invalide. Exemple : 1234567A/A/AAA/000";
  }

  return e;
}

// ── Password strength indicator ───────────────────────────────────────────────

function PasswordStrength({ password }: { password: string }) {
  const checks = [
    { label: "8 caractères minimum",      ok: password.length >= 8 },
    { label: "Une lettre majuscule",      ok: /[A-Z]/.test(password) },
    { label: "Un chiffre",               ok: /[0-9]/.test(password) },
    { label: "Un caractère spécial",     ok: /[^A-Za-z0-9]/.test(password) },
  ];
  if (!password) return null;
  return (
    <ul className="mt-2 space-y-1">
      {checks.map(c => (
        <li key={c.label} className={`flex items-center gap-1.5 text-xs ${c.ok ? "text-emerald-600" : "text-muted-foreground"}`}>
          {c.ok
            ? <CheckCircle2 className="h-3 w-3 text-emerald-600 shrink-0" />
            : <XCircle      className="h-3 w-3 text-muted-foreground/50 shrink-0" />
          }
          {c.label}
        </li>
      ))}
    </ul>
  );
}

// ── Field error ───────────────────────────────────────────────────────────────

function FieldError({ msg }: { msg?: string }) {
  if (!msg) return null;
  return <p className="text-xs text-destructive flex items-center gap-1 mt-1"><AlertCircle className="h-3 w-3" />{msg}</p>;
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function RegisterPage() {
  const { register } = useAuth();
  const navigate      = useNavigate();

  const [name,            setName]            = useState("");
  const [email,           setEmail]           = useState("");
  const [password,        setPassword]        = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword,    setShowPassword]    = useState(false);
  const [dob,             setDob]             = useState("");
  const [phone,           setPhone]           = useState("");
  const [role,            setRole]            = useState<"particular" | "agency">("particular");
  const [agencyName,      setAgencyName]      = useState("");
  const [matricule,       setMatricule]       = useState("");

  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [serverErrors, setServerErrors] = useState<string[]>([]);
  const [loading,     setLoading]     = useState(false);

  // Max date = today − 18 years
  const maxDob = (() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 18);
    return d.toISOString().split("T")[0];
  })();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setServerErrors([]);

    const clientErrors = validateClient({
      name, email, password, confirmPassword, dob,
      role, agencyName, matricule,
    });
    setFieldErrors(clientErrors);
    if (Object.keys(clientErrors).length > 0) return;

    setLoading(true);
    const result = await register({
      name,
      email,
      password,
      role,
      date_of_birth: dob,
      phone:         phone.trim() || undefined,
      ...(role === "agency" ? {
        agency_name:      agencyName.trim(),
        matricule_fiscale: matricule.trim().toUpperCase(),
      } : {}),
    });
    setLoading(false);

    if (result.ok) {
      toast.success("Compte créé ! Bienvenue sur EstateMind.");
      navigate("/user");
    } else {
      setServerErrors(result.errors ?? ["Une erreur est survenue."]);
    }
  };

  return (
    <PublicLayout>
      <div className="flex items-center justify-center min-h-[calc(100vh-12rem)] px-4 py-10">
        <Card className="w-full max-w-lg">
          <CardHeader className="text-center">
            <CardTitle className="text-2xl">Créer un compte</CardTitle>
            <CardDescription>Rejoignez EstateMind pour publier des annonces et accéder aux analyses.</CardDescription>
          </CardHeader>

          <CardContent>
            {serverErrors.length > 0 && (
              <Alert variant="destructive" className="mb-4">
                <AlertDescription>
                  <ul className="space-y-0.5 list-disc list-inside text-sm">
                    {serverErrors.map((e, i) => <li key={i}>{e}</li>)}
                  </ul>
                </AlertDescription>
              </Alert>
            )}

            <form onSubmit={handleSubmit} noValidate className="space-y-4">

              {/* Name */}
              <div className="space-y-1.5">
                <Label htmlFor="name">Nom complet</Label>
                <Input id="name" value={name} onChange={e => setName(e.target.value)}
                  placeholder="Mohamed Ben Ali" autoComplete="name" />
                <FieldError msg={fieldErrors.name} />
              </div>

              {/* Email */}
              <div className="space-y-1.5">
                <Label htmlFor="email">Email</Label>
                <Input id="email" type="email" value={email} onChange={e => setEmail(e.target.value)}
                  placeholder="vous@example.com" autoComplete="email" />
                <FieldError msg={fieldErrors.email} />
              </div>

              {/* Password */}
              <div className="space-y-1.5">
                <Label htmlFor="password">Mot de passe</Label>
                <div className="relative">
                  <Input id="password" type={showPassword ? "text" : "password"}
                    value={password} onChange={e => setPassword(e.target.value)}
                    placeholder="••••••••" autoComplete="new-password" className="pr-10" />
                  <button type="button"
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                    onClick={() => setShowPassword(v => !v)}>
                    {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                  </button>
                </div>
                <PasswordStrength password={password} />
                <FieldError msg={fieldErrors.password} />
              </div>

              {/* Confirm password */}
              <div className="space-y-1.5">
                <Label htmlFor="confirm">Confirmer le mot de passe</Label>
                <Input id="confirm" type={showPassword ? "text" : "password"}
                  value={confirmPassword} onChange={e => setConfirmPassword(e.target.value)}
                  placeholder="••••••••" autoComplete="new-password" />
                <FieldError msg={fieldErrors.confirmPassword} />
              </div>

              {/* Date of birth */}
              <div className="space-y-1.5">
                <Label htmlFor="dob">Date de naissance <span className="text-xs text-muted-foreground">(18 ans minimum)</span></Label>
                <Input id="dob" type="date" value={dob} onChange={e => setDob(e.target.value)}
                  max={maxDob} />
                <FieldError msg={fieldErrors.dob} />
              </div>

              {/* Phone (optional) */}
              <div className="space-y-1.5">
                <Label htmlFor="phone">Téléphone <span className="text-xs text-muted-foreground">(optionnel)</span></Label>
                <Input id="phone" type="tel" value={phone} onChange={e => setPhone(e.target.value)}
                  placeholder="+216 XX XXX XXX" autoComplete="tel" />
              </div>

              {/* Role */}
              <div className="space-y-2">
                <Label>Type de compte</Label>
                <RadioGroup value={role} onValueChange={v => setRole(v as typeof role)}
                  className="grid grid-cols-2 gap-3">
                  <Label htmlFor="role-particular"
                    className={`flex items-center gap-3 rounded-lg border p-3 cursor-pointer transition-colors
                      ${role === "particular" ? "border-primary bg-accent" : "hover:bg-muted"}`}>
                    <RadioGroupItem value="particular" id="role-particular" />
                    <User className="h-4 w-4" />
                    <span className="text-sm font-medium">Particulier</span>
                  </Label>
                  <Label htmlFor="role-agency"
                    className={`flex items-center gap-3 rounded-lg border p-3 cursor-pointer transition-colors
                      ${role === "agency" ? "border-primary bg-accent" : "hover:bg-muted"}`}>
                    <RadioGroupItem value="agency" id="role-agency" />
                    <Building2 className="h-4 w-4" />
                    <span className="text-sm font-medium">Agence</span>
                  </Label>
                </RadioGroup>
              </div>

              {/* Agency fields */}
              {role === "agency" && (
                <div className="space-y-4 rounded-xl border border-primary/20 bg-accent/30 p-4">
                  <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide">Informations agence</p>

                  <div className="space-y-1.5">
                    <Label htmlFor="agency-name">Nom de l'agence</Label>
                    <Input id="agency-name" value={agencyName} onChange={e => setAgencyName(e.target.value)}
                      placeholder="Immobilier El Amal" />
                    <FieldError msg={fieldErrors.agencyName} />
                  </div>

                  <div className="space-y-1.5">
                    <Label htmlFor="matricule">
                      Matricule fiscal
                      <span className="ml-1 text-xs text-muted-foreground">Format : 1234567A/A/AAA/000</span>
                    </Label>
                    <Input id="matricule"
                      value={matricule}
                      onChange={e => setMatricule(e.target.value.toUpperCase())}
                      placeholder="1234567A/A/AAA/000"
                      className="font-mono tracking-wider" />
                    <FieldError msg={fieldErrors.matricule} />
                  </div>
                </div>
              )}

              <Button type="submit" className="w-full" disabled={loading}>
                {loading ? "Création du compte…" : "Créer mon compte"}
              </Button>

              <p className="text-center text-sm text-muted-foreground">
                Déjà inscrit ?{" "}
                <Link to="/login" className="text-primary hover:underline">Se connecter</Link>
              </p>
            </form>
          </CardContent>
        </Card>
      </div>
    </PublicLayout>
  );
}