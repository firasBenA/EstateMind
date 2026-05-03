// // src/components/ClientMap.tsx
// // ─────────────────────────────────────────────────────────────────────────────
// // Pure-Leaflet map — no react-leaflet, no duplicate-React problem.
// //
// // WHY this approach:
// //   react-leaflet ships its own bundled copy of React. When Vite resolves
// //   imports it ends up with two React instances in the same page. The moment
// //   react-leaflet calls useState() it hits the wrong dispatcher → the
// //   "Invalid hook call / Cannot read properties of null (reading 'useState')"
// //   crash you're seeing in the console.
// //
// //   The fix: use Leaflet's imperative JavaScript API directly inside a
// //   useEffect + a ref. Zero React duplication, same visual result.
// // ─────────────────────────────────────────────────────────────────────────────

// import { useEffect, useRef } from "react";
// import L from "leaflet";
// import "leaflet/dist/leaflet.css";

// // Fix the broken default-marker icon paths that Vite's asset pipeline breaks
// delete (L.Icon.Default.prototype as any)._getIconUrl;
// L.Icon.Default.mergeOptions({
//   iconRetinaUrl:
//     "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png",
//   iconUrl:
//     "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png",
//   shadowUrl:
//     "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png",
// });

// interface ClientMapProps {
//   /** Initial center [lat, lng]. Defaults to Tunis. */
//   center?: [number, number];
//   zoom?: number;
//   height?: string;
//   /** Called with (lat, lng) every time the user clicks the map. */
//   onLocationSelect?: (lat: number, lng: number) => void;
// }

// export function ClientMap({
//   center = [36.8065, 10.1815],
//   zoom = 12,
//   height = "300px",
//   onLocationSelect,
// }: ClientMapProps) {
//   // mapRef → the DOM <div> that Leaflet will attach to
//   const mapRef   = useRef<HTMLDivElement>(null);
//   // instanceRef → the Leaflet Map instance (kept across renders)
//   const instanceRef = useRef<L.Map | null>(null);
//   // markerRef → the single draggable pin
//   const markerRef   = useRef<L.Marker | null>(null);

//   useEffect(() => {
//     // Guard: don't double-init if the effect runs twice (React StrictMode)
//     if (!mapRef.current || instanceRef.current) return;

//     // 1. Create the map
//     const map = L.map(mapRef.current, {
//       center,
//       zoom,
//       scrollWheelZoom: false,
//     });
//     instanceRef.current = map;

//     // 2. Add OpenStreetMap tiles
//     L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
//       attribution:
//         '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
//     }).addTo(map);

//     // 3. Click handler → place / move the marker
//     map.on("click", (e: L.LeafletMouseEvent) => {
//       const { lat, lng } = e.latlng;

//       if (markerRef.current) {
//         // Move existing marker
//         markerRef.current.setLatLng([lat, lng]);
//       } else {
//         // Create marker on first click
//         markerRef.current = L.marker([lat, lng]).addTo(map);
//       }

//       onLocationSelect?.(lat, lng);
//     });

//     // Cleanup: destroy the map when the component unmounts so Leaflet
//     // releases its internal event listeners and DOM nodes.
//     return () => {
//       map.remove();
//       instanceRef.current = null;
//       markerRef.current   = null;
//     };
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, []); // empty deps → run once on mount

//   // If the consumer changes `center` after mount, fly there smoothly
//   useEffect(() => {
//     instanceRef.current?.setView(center, zoom);
//   }, [center, zoom]);

//   return (
//     <div
//       ref={mapRef}
//       style={{ height, width: "100%" }}
//       // z-0 keeps the map tiles below every shadcn/radix popover/dropdown
//       className="rounded-md overflow-hidden border z-0"
//     />
//   );
// }

// frontend-client/src/components/ClientMap.tsx

import { useEffect, useRef, useImperativeHandle, forwardRef } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

// Fix marker icons
delete (L.Icon.Default.prototype as any)._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png",
  iconUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png",
  shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png",
});

export interface ClientMapRef {
  flyTo: (lat: number, lng: number, zoom?: number) => void;
  setView: (lat: number, lng: number, zoom?: number) => void;
  getCenter: () => [number, number];
  getZoom: () => number;
}

interface ClientMapProps {
  initialCenter?: [number, number];
  initialZoom?: number;
  height?: string;
  onLocationSelect?: (lat: number, lng: number) => void;
  onMapReady?: (map: L.Map) => void;
}

export const ClientMap = forwardRef<ClientMapRef, ClientMapProps>(({
  initialCenter = [36.8065, 10.1815],
  initialZoom = 12,
  height = "300px",
  onLocationSelect,
  onMapReady,
}, ref) => {
  const mapRef = useRef<HTMLDivElement>(null);
  const instanceRef = useRef<L.Map | null>(null);
  const markerRef = useRef<L.Marker | null>(null);

  // Expose methods to parent via ref
  useImperativeHandle(ref, () => ({
    flyTo: (lat: number, lng: number, zoom?: number) => {
      instanceRef.current?.flyTo([lat, lng], zoom || instanceRef.current.getZoom());
    },
    setView: (lat: number, lng: number, zoom?: number) => {
      instanceRef.current?.setView([lat, lng], zoom || instanceRef.current.getZoom());
    },
    getCenter: (): [number, number] => {
      const center = instanceRef.current?.getCenter();
      return center ? [center.lat, center.lng] : [0, 0];
    },
    getZoom: (): number => {
      return instanceRef.current?.getZoom() || 0;
    },
  }));

  useEffect(() => {
    if (!mapRef.current || instanceRef.current) return;

    const map = L.map(mapRef.current, {
      center: initialCenter,
      zoom: initialZoom,
      scrollWheelZoom: false,
    });
    instanceRef.current = map;

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(map);

    map.on("click", (e: L.LeafletMouseEvent) => {
      const { lat, lng } = e.latlng;

      if (markerRef.current) {
        markerRef.current.setLatLng([lat, lng]);
      } else {
        markerRef.current = L.marker([lat, lng]).addTo(map);
      }

      onLocationSelect?.(lat, lng);
    });

    onMapReady?.(map);

    return () => {
      map.remove();
      instanceRef.current = null;
      markerRef.current = null;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div
      ref={mapRef}
      style={{ height, width: "100%" }}
      className="rounded-md overflow-hidden border z-0"
    />
  );
});

ClientMap.displayName = "ClientMap";