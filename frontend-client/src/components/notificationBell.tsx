// frontend-client/src/components/NotificationBell.tsx
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Link } from 'react-router-dom';
import { Bell } from 'lucide-react';
import Pusher from 'pusher-js';
import { useAuth } from '@/lib/auth-context';

interface Notification {
  id: number;
  type: 'new_listing' | 'price_drop' | 'similar_listing';
  title: string;
  message: string;
  listing_id: string | null;
  is_read: boolean;
  created_at: string;
  data?: Record<string, any>;
}

function getCsrfToken(): string {
  const match = document.cookie.match(/(^|;\s*)csrftoken=([^;]*)/);
  return match ? decodeURIComponent(match[2]) : '';
}

const NotificationBell: React.FC = () => {
  const { user, isAuthenticated } = useAuth();
  const [notifications, setNotifications] = useState<Notification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [isOpen, setIsOpen] = useState(false);
  const pusherRef = useRef<Pusher | null>(null);
  const channelRef = useRef<any>(null);

  // Request browser notification permission
  useEffect(() => {
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }
  }, []);

  // Fetch notifications from backend
  const fetchNotifications = useCallback(async () => {
    if (!isAuthenticated) return;
    try {
      const res = await fetch('/api/notifications/?limit=20', {
        credentials: 'include',
      });
      if (!res.ok) return;
      const data = await res.json();
      setNotifications(data.notifications || []);
      setUnreadCount(data.unread_count || 0);
    } catch (err) {
      console.error('Failed to fetch notifications:', err);
    }
  }, [isAuthenticated]);

  // Add a notification to state + show browser notification
  const addNotification = useCallback((raw: any) => {
    const n: Notification = {
      id: raw.id || Date.now(),
      type: raw.type || 'new_listing',
      title: raw.title,
      message: raw.message,
      listing_id: raw.listing_id ?? null,
      is_read: false,
      created_at: raw.created_at || new Date().toISOString(),
      data: raw.data,
    };
    setNotifications(prev => [n, ...prev]);
    setUnreadCount(prev => prev + 1);

    if (Notification.permission === 'granted') {
      new Notification(n.title, {
        body: n.message,
        icon: '/favicon.ico',
        tag: String(n.id),
      });
    }
  }, []);

  // Setup Pusher once user is known
  useEffect(() => {
    if (!isAuthenticated || !user?.id) return;

    const pusherKey = import.meta.env.VITE_PUSHER_KEY;
    const pusherCluster = import.meta.env.VITE_PUSHER_CLUSTER || 'eu';

    if (!pusherKey) {
      console.warn('VITE_PUSHER_KEY not set — real-time notifications disabled');
      return;
    }

    // Clean up any existing connection
    if (channelRef.current) {
      channelRef.current.unbind_all();
      channelRef.current = null;
    }
    if (pusherRef.current) {
      pusherRef.current.disconnect();
      pusherRef.current = null;
    }

    pusherRef.current = new Pusher(pusherKey, {
      cluster: pusherCluster,
      // ✅ Use session-based auth (same as chat)
      authEndpoint: '/api/pusher/auth/',
      auth: {
        headers: { 'X-CSRFToken': getCsrfToken() },
      },
    });

    // ✅ Private channel requires "private-" prefix + Pusher auth
    const channelName = `private-user-${user.id}`;
    channelRef.current = pusherRef.current.subscribe(channelName);

    channelRef.current.bind('new-listing',      addNotification);
    channelRef.current.bind('price-drop',        addNotification);
    channelRef.current.bind('similar-listing',   addNotification);

    channelRef.current.bind('pusher:subscription_error', (err: any) => {
      console.error('Pusher subscription error:', err);
    });

    fetchNotifications();

    return () => {
      channelRef.current?.unbind_all();
      pusherRef.current?.disconnect();
    };
  }, [isAuthenticated, user?.id, addNotification, fetchNotifications]);

  const markAsRead = async (id: number) => {
    try {
      await fetch(`/api/notifications/${id}/read/`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'X-CSRFToken': getCsrfToken() },
      });
      setNotifications(prev => prev.map(n => n.id === id ? { ...n, is_read: true } : n));
      setUnreadCount(prev => Math.max(0, prev - 1));
    } catch (err) {
      console.error('Failed to mark notification as read:', err);
    }
  };

  const markAllAsRead = async () => {
    const unread = notifications.filter(n => !n.is_read);
    await Promise.all(unread.map(n => markAsRead(n.id)));
  };

  const getIcon = (type: string) => {
    switch (type) {
      case 'new_listing':     return '🏠';
      case 'price_drop':      return '💰';
      case 'similar_listing': return '🔍';
      default:                return '🔔';
    }
  };

  if (!isAuthenticated) return null;

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(v => !v)}
        className="relative p-2 rounded-full hover:bg-gray-100 transition-colors"
        aria-label="Notifications"
      >
        <Bell className="w-5 h-5 text-gray-600" />
        {unreadCount > 0 && (
          <span className="absolute top-0 right-0 bg-red-500 text-white text-[10px] font-bold rounded-full w-4 h-4 flex items-center justify-center animate-pulse">
            {unreadCount > 9 ? '9+' : unreadCount}
          </span>
        )}
      </button>

      {isOpen && (
        <>
          {/* Backdrop */}
          <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />

          {/* Dropdown */}
          <div className="absolute right-0 mt-2 w-96 bg-white rounded-xl shadow-2xl z-50 max-h-[500px] overflow-hidden border">
            <div className="p-3 border-b flex justify-between items-center bg-gray-50 rounded-t-xl">
              <h3 className="font-semibold text-sm">Notifications</h3>
              {unreadCount > 0 && (
                <button
                  onClick={markAllAsRead}
                  className="text-xs text-blue-600 hover:text-blue-800"
                >
                  Mark all as read
                </button>
              )}
            </div>

            <div className="overflow-y-auto max-h-[420px] divide-y">
              {notifications.length === 0 ? (
                <div className="p-8 text-center text-gray-400">
                  <span className="text-4xl">🔔</span>
                  <p className="mt-2 text-sm">No notifications yet</p>
                </div>
              ) : (
                notifications.map(n => (
                  <div
                    key={n.id}
                    onClick={() => markAsRead(n.id)}
                    className={`p-3 cursor-pointer hover:bg-gray-50 transition ${!n.is_read ? 'bg-blue-50' : ''}`}
                  >
                    <div className="flex items-start gap-3">
                      <span className="text-xl shrink-0">{getIcon(n.type)}</span>
                      <div className="flex-1 min-w-0">
                        <p className="font-semibold text-sm">{n.title}</p>
                        <p className="text-xs text-gray-600 mt-0.5">{n.message}</p>
                        {n.listing_id && (
                          <Link
                            to={`/listing/${n.listing_id}`}
                            className="text-xs text-blue-600 hover:underline mt-1 inline-block"
                            onClick={e => e.stopPropagation()}
                          >
                            View property →
                          </Link>
                        )}
                        <p className="text-[10px] text-gray-400 mt-1">
                          {new Date(n.created_at).toLocaleString()}
                        </p>
                      </div>
                      {!n.is_read && (
                        <div className="w-2 h-2 bg-blue-500 rounded-full mt-1 shrink-0" />
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default NotificationBell;