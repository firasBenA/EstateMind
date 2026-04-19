// frontend-client/src/lib/auth-context.tsx

import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

interface User {
  id?: number;
  username: string;
  email?: string;
  name?: string;
  role?: string;
  is_superuser?: boolean;
  last_login?: string;
}

interface RegisterData {
  name: string;
  email: string;
  password: string;
  role: "particular" | "agency";
  date_of_birth: string;
  phone?: string;
  agency_name?: string;
  matricule_fiscale?: string;
}

interface RegisterResult {
  ok: boolean;
  errors?: string[];
}

interface AuthContextType {
  isAuthenticated: boolean;
  user: User | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (data: RegisterData) => Promise<RegisterResult>;
  logout: () => Promise<void>;
  getAccessToken: () => Promise<string | null>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Session check interval (5 minutes)
const SESSION_CHECK_INTERVAL = 5 * 60 * 1000;

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  // Check session function
  const checkSession = useCallback(async () => {
    try {
      const response = await fetch('/api/session/', {
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        }
      });
      
      if (response.ok) {
        const data = await response.json();
        if (data.is_authenticated) {
          setIsAuthenticated(true);
          setUser({
            id: data.id,
            username: data.username,
            email: data.email,
            name: data.name,
            role: data.role,
            is_superuser: data.is_superuser,
            last_login: data.last_login,
          });
          return true;
        }
      }
      
      setIsAuthenticated(false);
      setUser(null);
      return false;
      
    } catch (error) {
      console.error('Session check failed:', error);
      setIsAuthenticated(false);
      setUser(null);
      return false;
    }
  }, []);

  // Initial session check
  useEffect(() => {
    const initAuth = async () => {
      setLoading(true);
      await checkSession();
      setLoading(false);
    };
    initAuth();
  }, [checkSession]);

  // Periodic session check (every 5 minutes)
  useEffect(() => {
    if (!isAuthenticated) return;
    
    const intervalId = setInterval(async () => {
      const stillValid = await checkSession();
      if (!stillValid) {
        window.location.href = '/login';
      }
    }, SESSION_CHECK_INTERVAL);
    
    return () => clearInterval(intervalId);
  }, [isAuthenticated, checkSession]);

  // Activity tracker
  useEffect(() => {
    let activityTimeout: NodeJS.Timeout;
    
    const resetActivityTimer = () => {
      if (activityTimeout) clearTimeout(activityTimeout);
      activityTimeout = setTimeout(async () => {
        await checkSession();
      }, 30 * 60 * 1000);
    };
    
    const events = ['mousedown', 'keydown', 'scroll', 'touchstart'];
    events.forEach(event => {
      window.addEventListener(event, resetActivityTimer);
    });
    
    resetActivityTimer();
    
    return () => {
      events.forEach(event => {
        window.removeEventListener(event, resetActivityTimer);
      });
      if (activityTimeout) clearTimeout(activityTimeout);
    };
  }, [checkSession]);

  const login = async (email: string, password: string) => {
    const response = await fetch('/api/login/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'include',
      body: JSON.stringify({ email, password }),
    });
    
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Login failed');
    }
    
    const data = await response.json();
    setIsAuthenticated(true);
    setUser({
      id: data.id,
      username: data.username,
      email: data.email,
      name: data.name,
      role: data.role,
      is_superuser: data.is_superuser,
    });
  };

  const register = async (userData: RegisterData): Promise<RegisterResult> => {
    try {
      const response = await fetch('/api/register/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(userData),
      });
      
      const data = await response.json();
      
      if (!response.ok) {
        // Handle validation errors from backend
        if (data.errors) {
          return { ok: false, errors: data.errors };
        }
        return { ok: false, errors: [data.error || "Registration failed"] };
      }
      
      // Registration successful - user is now logged in
      setIsAuthenticated(true);
      setUser({
        id: data.id,
        username: data.username,
        email: data.email,
        name: data.name,
        role: data.role,
        is_superuser: data.is_superuser,
      });
      
      return { ok: true };
      
    } catch (error) {
      console.error('Registration error:', error);
      return { ok: false, errors: ["Network error. Please try again."] };
    }
  };

  const logout = async () => {
    await fetch('/api/logout/', {
      method: 'POST',
      credentials: 'include',
    });
    setIsAuthenticated(false);
    setUser(null);
    window.location.href = '/';
  };

  const getAccessToken = async () => {
    const isValid = await checkSession();
    return isValid ? 'session-valid' : null;
  };

  return (
    <AuthContext.Provider value={{ 
      isAuthenticated, 
      user, 
      loading, 
      login, 
      register, 
      logout, 
      getAccessToken 
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}