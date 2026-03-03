import { useCallback, useEffect, useMemo, useState, type FC } from "react";
import {
  ReactFlow,
  Background,
  Position,
  type Node,
  type Edge,
  type NodeTypes,
  MarkerType,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { fetchDevelopments } from "../api/developments";
import type { DevEvent, DevEdge } from "../types/development";
import EventNode from "../components/EventNode";
import { formatLocalTime } from "../utils/time";

const nodeTypes: NodeTypes = { event: EventNode };

const IMPACT_BORDER: Record<string, string> = {
  critical: "#f85149",
  high: "#e3952d",
  medium: "#d29922",
};

const RELATION_EDGE_COLOR: Record<string, string> = {
  escalation: "#f85149",
  retaliation: "#a371f7",
  response: "#4ea1ff",
  continuation: "#6e7681",
  political_reaction: "#d29922",
  strategic_signal: "#39c5cf",
};

const DEFAULT_EDGE_COLOR = "#6e7681";

const X_STEP = 400;
const Y_STEP = 180;
const COMPONENT_GAP = 280;

function findConnectedComponents(
  nodeIds: string[],
  edges: { source: string; target: string }[],
): string[][] {
  const adj = new Map<string, string[]>();
  for (const id of nodeIds) adj.set(id, []);
  for (const e of edges) {
    adj.get(e.source)!.push(e.target);
    adj.get(e.target)!.push(e.source);
  }
  const visited = new Set<string>();
  const components: string[][] = [];
  for (const id of nodeIds) {
    if (visited.has(id)) continue;
    const comp: string[] = [];
    const stack = [id];
    while (stack.length > 0) {
      const cur = stack.pop()!;
      if (visited.has(cur)) continue;
      visited.add(cur);
      comp.push(cur);
      for (const n of adj.get(cur) ?? []) {
        if (!visited.has(n)) stack.push(n);
      }
    }
    components.push(comp);
  }
  return components;
}

function computeDepth(
  nodeIds: string[],
  edges: { source: string; target: string }[],
): Map<string, number> {
  const depth = new Map<string, number>();
  for (const id of nodeIds) depth.set(id, 0);

  for (let pass = 0; pass < nodeIds.length; pass++) {
    for (const e of edges) {
      const srcDepth = depth.get(e.source) ?? 0;
      const tgtDepth = depth.get(e.target) ?? 0;
      depth.set(e.target, Math.max(tgtDepth, srcDepth + 1));
    }
  }
  return depth;
}

function layoutNodes(apiEvents: DevEvent[], apiEdges: DevEdge[]): Node[] {
  const edgeList = apiEdges.map((e) => ({
    source: e.source_event_id,
    target: e.target_event_id,
  }));

  const connectedIds = new Set<string>();
  for (const e of edgeList) {
    connectedIds.add(e.source);
    connectedIds.add(e.target);
  }

  const connectedEvents = apiEvents.filter((e) => connectedIds.has(e.id));
  const unconnectedEvents = apiEvents.filter((e) => !connectedIds.has(e.id));

  const eventById = new Map(apiEvents.map((e) => [e.id, e]));

  const components = findConnectedComponents(
    connectedEvents.map((e) => e.id),
    edgeList,
  );

  const nodes: Node[] = [];
  let currentY = 0;

  for (const compIds of components) {
    const compEdges = edgeList.filter(
      (e) => compIds.includes(e.source) && compIds.includes(e.target),
    );
    const depthMap = computeDepth(compIds, compEdges);

    const byDepth = new Map<number, DevEvent[]>();
    for (const id of compIds) {
      const ev = eventById.get(id)!;
      const d = depthMap.get(id) ?? 0;
      const list = byDepth.get(d) ?? [];
      list.push(ev);
      byDepth.set(d, list);
    }

    const depths = [...byDepth.keys()].sort((a, b) => a - b);
    let compMaxY = 0;

    for (const depth of depths) {
      const eventsAtDepth = byDepth.get(depth) ?? [];
      eventsAtDepth.sort(
        (a, b) =>
          new Date(a.first_seen).getTime() - new Date(b.first_seen).getTime(),
      );
      for (let i = 0; i < eventsAtDepth.length; i++) {
        const n = eventsAtDepth[i];
        const y = currentY + i * Y_STEP;
        compMaxY = Math.max(compMaxY, y);
        nodes.push({
          id: n.id,
          type: "event",
          position: { x: depth * X_STEP, y },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
          data: {
            title: n.title,
            impact_level: n.impact_level,
            confidence: n.confidence,
            actors: n.actors ?? [],
            first_seen: n.first_seen ? formatLocalTime(n.first_seen) : "",
            borderColor: IMPACT_BORDER[n.impact_level] ?? "#d29922",
          },
        });
      }
    }

    currentY = compMaxY + COMPONENT_GAP;
  }

  const unconnectedStartY = currentY;
  unconnectedEvents.sort(
    (a, b) => new Date(a.first_seen).getTime() - new Date(b.first_seen).getTime(),
  );
  for (let i = 0; i < unconnectedEvents.length; i++) {
    const n = unconnectedEvents[i];
    nodes.push({
      id: n.id,
      type: "event",
      position: { x: 0, y: unconnectedStartY + i * Y_STEP },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        title: n.title,
        impact_level: n.impact_level,
        confidence: n.confidence,
        actors: n.actors ?? [],
        first_seen: n.first_seen ? formatLocalTime(n.first_seen) : "",
        borderColor: IMPACT_BORDER[n.impact_level] ?? "#d29922",
      },
    });
  }

  return nodes;
}

function pickHandlePair(
  srcPos: { x: number; y: number },
  tgtPos: { x: number; y: number },
): { sourceHandle: string; targetHandle: string } {
  const dx = tgtPos.x - srcPos.x;
  const dy = tgtPos.y - srcPos.y;
  const absDx = Math.abs(dx);
  const absDy = Math.abs(dy);

  if (dx > 0 && absDx >= absDy) {
    return { sourceHandle: "source-right", targetHandle: "target-left" };
  }
  if (dx < 0 && absDx >= absDy) {
    return { sourceHandle: "source-left", targetHandle: "target-right" };
  }
  if (dy === 0 && dx === 0) {
    return { sourceHandle: "source-right", targetHandle: "target-left" };
  }
  if (dy > 0) {
    return { sourceHandle: "source-bottom", targetHandle: "target-top" };
  }
  return { sourceHandle: "source-top", targetHandle: "target-bottom" };
}

function buildEdges(apiEdges: DevEdge[], nodes: Node[]): Edge[] {
  const posById = new Map(nodes.map((n) => [n.id, n.position]));
  const byPair = new Map<string, DevEdge>();
  for (const e of apiEdges) {
    const a = String(e.source_event_id);
    const b = String(e.target_event_id);
    const key = a < b ? `${a}::${b}` : `${b}::${a}`;
    if (!byPair.has(key)) byPair.set(key, e);
  }
  const deduped = [...byPair.values()];
  return deduped.map((e, i) => {
    const color = RELATION_EDGE_COLOR[e.relation_type] ?? DEFAULT_EDGE_COLOR;
    const srcPos = posById.get(e.source_event_id) ?? { x: 0, y: 0 };
    const tgtPos = posById.get(e.target_event_id) ?? { x: 0, y: 0 };
    const { sourceHandle, targetHandle } = pickHandlePair(srcPos, tgtPos);
    return {
      id: `e-${i}`,
      source: e.source_event_id,
      target: e.target_event_id,
      sourceHandle,
      targetHandle,
      type: "smoothstep",
      animated: false,
      style: { stroke: color, strokeWidth: 1.5 },
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 16, height: 16 },
    };
  });
}

const DevelopmentsPage: FC = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastFetched, setLastFetched] = useState<Date | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchDevelopments();
      const layoutedNodes = layoutNodes(data.events, data.edges);
      setNodes(layoutedNodes);
      setEdges(buildEdges(data.edges, layoutedNodes));
      setLastFetched(new Date());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch developments");
    } finally {
      setLoading(false);
    }
  }, [setNodes, setEdges]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    document.documentElement.style.overflow = "hidden";
    document.body.style.overflow = "hidden";
    return () => {
      document.documentElement.style.overflow = "";
      document.body.style.overflow = "";
    };
  }, []);

  const proOptions = useMemo(() => ({ hideAttribution: true }), []);

  return (
    <div
      className="flex flex-col overflow-hidden"
      style={{ height: "calc(100vh - 52px)" }}
    >
      {/* Toolbar */}
      <div
        className="flex items-center gap-4 px-4 py-2 border-b shrink-0"
        style={{
          backgroundColor: "var(--color-bg-card)",
          borderColor: "var(--color-border)",
        }}
      >
        <button
          onClick={load}
          disabled={loading}
          className="px-3 py-1.5 rounded text-xs font-medium uppercase tracking-wide border transition-colors"
          style={{
            borderColor: "var(--color-accent-blue)",
            color: loading ? "var(--color-text-secondary)" : "var(--color-accent-blue)",
            backgroundColor: "transparent",
          }}
        >
          {loading ? "Loading…" : "Refresh"}
        </button>

        {error && (
          <span className="text-xs" style={{ color: "var(--color-accent-red)" }}>
            {error}
          </span>
        )}

        <span
          className="ml-auto text-xs tabular-nums"
          style={{ color: "var(--color-text-secondary)" }}
        >
          {lastFetched
            ? `${nodes.length} events · ${edges.length} links`
            : "—"}
        </span>

        {/* Legend: impact */}
        <div className="flex items-center gap-3">
          {(["critical", "high", "medium"] as const).map((level) => (
            <span key={level} className="flex items-center gap-1 text-xs">
              <span
                className="inline-block w-2.5 h-2.5 rounded-sm"
                style={{ backgroundColor: IMPACT_BORDER[level] }}
              />
              <span style={{ color: "var(--color-text-secondary)" }}>
                {level}
              </span>
            </span>
          ))}
        </div>

        {/* Legend: relation types */}
        <div className="flex items-center gap-2 text-xs">
          {Object.entries(RELATION_EDGE_COLOR).map(([type, color]) => (
            <span key={type} className="flex items-center gap-1">
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ backgroundColor: color }}
              />
              <span style={{ color: "var(--color-text-secondary)" }}>
                {type.replace(/_/g, " ")}
              </span>
            </span>
          ))}
        </div>
      </div>

      {/* Graph */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          proOptions={proOptions}
          nodesDraggable={false}
          fitView
          minZoom={0.1}
          maxZoom={2}
          style={{ backgroundColor: "var(--color-bg-base)" }}
        >
          <Background color="#242a35" gap={24} size={1} />
        </ReactFlow>
      </div>
    </div>
  );
};

export default DevelopmentsPage;
