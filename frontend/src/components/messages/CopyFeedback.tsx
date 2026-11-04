import { Check, CircleAlert } from "lucide-react";
import "./feedback.css";

export function CopyFeedback({ message, error, sequence, className = "" }: {
  message: string;
  error: boolean;
  sequence: number;
  className?: string;
}) {
  return <div className={`message-copy-feedback ${className}${message ? " is-visible" : ""}`} data-tone={error ? "error" : undefined} role="status" aria-live="polite" aria-atomic="true">
    {message && <span key={sequence}>{error ? <CircleAlert aria-hidden="true" /> : <Check aria-hidden="true" />}{message}</span>}
  </div>;
}
