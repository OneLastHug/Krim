import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { PrototypeApp } from "./PrototypeApp";
import "@fontsource-variable/geist";
import "@fontsource-variable/geist/wght-italic.css";
import "@fontsource-variable/geist-mono";
import "@fontsource-variable/geist-mono/wght-italic.css";
import "@fontsource-variable/noto-sans-sc";
import "./prototype.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode><PrototypeApp /></StrictMode>,
);
