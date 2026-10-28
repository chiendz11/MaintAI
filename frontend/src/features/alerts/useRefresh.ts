import { useEffect, useState } from "react";
import { changes } from "../../realtime/alerts";

export function useAlertRefresh(): [number, () => void] {
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const refresh = () => {
      if (document.visibilityState === "visible") setRevision((v) => v + 1);
    };
    const timer = setInterval(refresh, 30000);
    changes.addEventListener("change", refresh);
    document.addEventListener("visibilitychange", refresh);
    return () => {
      clearInterval(timer);
      changes.removeEventListener("change", refresh);
      document.removeEventListener("visibilitychange", refresh);
    };
  }, []);
  return [revision, () => setRevision((v) => v + 1)];
}
