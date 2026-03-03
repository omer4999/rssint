import { useState, type FC } from "react";
import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Header from "./components/Header";
import HourlyBriefBar from "./components/HourlyBriefBar";
import FeedPage from "./pages/FeedPage";
import DevelopmentsPage from "./pages/DevelopmentsPage";
import AnalysisPage from "./pages/AnalysisPage";

const App: FC = () => {
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  return (
    <BrowserRouter>
      <div
        className="min-h-screen pb-16"
        style={{ backgroundColor: "var(--color-bg-base)" }}
      >
        <Header lastUpdated={lastUpdated} />

        {/* Nav tabs */}
        <nav
          className="border-b"
          style={{
            borderColor: "var(--color-border)",
            backgroundColor: "var(--color-bg-base)",
          }}
        >
          <div className="max-w-[900px] mx-auto px-4 flex gap-1">
            <TabLink to="/">Feed</TabLink>
            <TabLink to="/developments">Developments</TabLink>
            <TabLink to="/analysis">Analysis</TabLink>
          </div>
        </nav>

        <Routes>
          <Route path="/" element={<FeedPage onLastUpdated={setLastUpdated} />} />
          <Route path="/developments" element={<DevelopmentsPage />} />
          <Route path="/analysis" element={<AnalysisPage />} />
        </Routes>

        <HourlyBriefBar />
      </div>
    </BrowserRouter>
  );
};

const TabLink: FC<{ to: string; children: React.ReactNode }> = ({
  to,
  children,
}) => (
  <NavLink
    to={to}
    className={({ isActive }) =>
      `px-4 py-2 text-xs font-medium uppercase tracking-wide border-b-2 transition-colors ${
        isActive ? "" : "border-transparent"
      }`
    }
    style={({ isActive }) => ({
      color: isActive ? "var(--color-accent-blue)" : "var(--color-text-secondary)",
      borderBottomColor: isActive ? "var(--color-accent-blue)" : "transparent",
    })}
  >
    {children}
  </NavLink>
);

export default App;
