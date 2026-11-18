import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles/fonts.css";
import "./index.css";
import "./styles/workbench-refinements.css";
import "./styles/approved-workbench.css";
import "./styles/approved-workbench-adapters.css";
createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
