/**
 * frontend-client/src/lib/auth-context.tsx
 *
 * AuthContext backed by the real Django REST API.
 * - Session is verified on mount via GET /api/session/
 * - Passwords are NEVER stored client-side
 * - User object comes directly from the server
 */
import React, {
  createContext, useContext, useState,
  useEffect, useCallback,
} from "react";
import { authApi, type SessionUser, type RegisterPayload, ApiError } from "./api";

export type UserRole = "particular" | "agency" | "analyst" | "admin";

export interface AuthUser {
  id:          number;
  username:    string;
  email:       string;
  name:        string;
  role:        UserRole;
  is_superuser: boolean;
}

interface AuthContextType {
  user:            AuthUser | null;
  loading:         boolean;           // true while checking session on mount
  isAuthenticated: boolean;
  login:    (email: string, password: string) => Promise<{ ok: boolean; error?: string }>;
  register: (payload: RegisterPayload) => Promise<{ ok: boolean; errors?: string[] }>;
  logout:   () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | null>(null);

function sessionToUser(s: SessionUser): AuthUser {
  return {
    id:          s.id,
    username:    s.username,
    email:       s.email,
    name:        s.name,
    role:        s.role,
    is_superuser: s.is_superuser,
  };
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser]       = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);

  // Verify session on mount (handles page refresh)
  useEffect(() => {
    authApi.session()
      .then(s => setUser(sessionToUser(s)))
      .catch(() => setUser(null))
      .finally(() => setLoading(false));
  }, []);

  const login = useCallback(async (
    email: string,
    password: string,
  ): Promise<{ ok: boolean; error?: string }> => {
    try {
      const s = await authApi.login(email, password);
      setUser(sessionToUser(s));
      return { ok: true };
    } catch (err) {
      const msg = err instanceof ApiError ? err.message : "Erreur réseau.";
      return { ok: false, error: msg };
    }
  }, []);

  const register = useCallback(async (
    payload: RegisterPayload,
  ): Promise<{ ok: boolean; errors?: string[] }> => {
    try {
      const s = await authApi.register(payload);
      setUser(sessionToUser(s));
      return { ok: true };
    } catch (err) {
      if (err instanceof ApiError) {
        const body = err.body as { errors?: string[]; error?: string };
        return {
          ok: false,
          errors: body.errors ?? [body.error ?? err.message],
        };
      }
      return { ok: false, errors: ["Erreur réseau. Veuillez réessayer."] };
    }
  }, []);

  const logout = useCallback(async () => {
    await authApi.logout().catch(() => {});
    setUser(null);
  }, []);

  return (
    <AuthContext.Provider
      value={{ user, loading, isAuthenticated: !!user, login, register, logout }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}