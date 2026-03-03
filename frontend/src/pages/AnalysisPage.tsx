import { useCallback, useEffect, useState, type FC } from "react";
import { fetchAnalysis } from "../api/analysis";

const AnalysisPage: FC = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [conflictOverview, setConflictOverview] = useState<string>("");
  const [latestDevelopments, setLatestDevelopments] = useState<string>("");
  const [possibleOutcomes, setPossibleOutcomes] = useState<string>("");
  const [generatedAt, setGeneratedAt] = useState<string>("");

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetchAnalysis();
      setConflictOverview(result.conflict_overview);
      setLatestDevelopments(result.latest_developments);
      setPossibleOutcomes(result.possible_outcomes);
      setGeneratedAt(result.generated_at);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load analysis");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const hasContent =
    conflictOverview || latestDevelopments || possibleOutcomes;

  return (
    <main className="max-w-[900px] mx-auto px-4 py-6">
      {/* Header */}
      <div className="mb-8">
        <h1
          className="text-lg font-semibold"
          style={{ color: "var(--color-text-primary)" }}
        >
          Conflict Analysis
        </h1>
        <p
          className="text-sm mt-0.5"
          style={{ color: "var(--color-text-secondary)" }}
        >
          AI-generated analysis of the developments graph (updated every 6 hours)
        </p>
      </div>

      {loading && (
        <div
          className="text-center py-16 rounded-lg border"
          style={{
            borderColor: "var(--color-border)",
            color: "var(--color-text-secondary)",
            backgroundColor: "var(--color-bg-card)",
          }}
        >
          <p className="text-sm">Loading analysis…</p>
        </div>
      )}

      {error && !loading && (
        <div
          className="mb-6 p-4 rounded-lg border text-sm"
          style={{
            borderColor: "var(--color-accent-red)",
            color: "var(--color-accent-red)",
            backgroundColor: "rgba(248, 81, 73, 0.08)",
          }}
        >
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          {generatedAt && (
            <p
              className="text-xs mb-6"
              style={{ color: "var(--color-text-secondary)" }}
            >
              Last updated {new Date(generatedAt).toLocaleString()}
            </p>
          )}

          {hasContent ? (
            <div className="space-y-8">
              {conflictOverview && (
                <Section
                  title="Conflict Overview"
                  content={conflictOverview}
                />
              )}

              {latestDevelopments && (
                <Section
                  title="Latest Developments"
                  content={latestDevelopments}
                />
              )}

              {possibleOutcomes && (
                <Section
                  title="Possible Outcomes"
                  content={possibleOutcomes}
                  isSpeculative
                />
              )}
            </div>
          ) : (
            <div
              className="text-center py-16 rounded-lg border"
              style={{
                borderColor: "var(--color-border)",
                color: "var(--color-text-secondary)",
                backgroundColor: "var(--color-bg-card)",
              }}
            >
              <p className="text-sm">No analysis available yet.</p>
              <p className="text-xs mt-2">
                The first analysis will be generated within 6 hours of startup.
              </p>
            </div>
          )}
        </>
      )}
    </main>
  );
};

const Section: FC<{
  title: string;
  content: string;
  isSpeculative?: boolean;
}> = ({ title, content, isSpeculative }) => (
  <section
    className="rounded-lg border p-5"
    style={{
      borderColor: "var(--color-border)",
      backgroundColor: "var(--color-bg-card)",
    }}
  >
    <h2
      className="text-sm font-semibold uppercase tracking-wide mb-3"
      style={{
        color: isSpeculative ? "var(--color-accent-yellow)" : "var(--color-text-primary)",
      }}
    >
      {title}
    </h2>
    <div
      className="text-sm leading-relaxed whitespace-pre-wrap"
      style={{ color: "var(--color-text-primary)" }}
    >
      {content}
    </div>
  </section>
);

export default AnalysisPage;
