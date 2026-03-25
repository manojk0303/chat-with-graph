import { useEffect, useRef, useState, useCallback } from 'react'
import ForceGraph3D from 'react-force-graph-3d'
import axios from 'axios'

const NODE_COLORS = {
    Customer: '#9333ea',        // Purple
    SalesOrder: '#2563eb',      // Blue
    Delivery: '#059669',        // Green
    BillingDocument: '#dc2626', // Red
    JournalEntry: '#d97706',    // Orange
    Payment: '#16a34a',         // Emerald
}

const NODE_SIZE = {
    Customer: 18,
    SalesOrder: 7,
    Delivery: 6,
    BillingDocument: 8,
    JournalEntry: 5,
    Payment: 5,
}

export default function GraphView({ highlightedIds = [] }) {
    const fgRef = useRef()
    const containerRef = useRef()
    const [dims, setDims] = useState({ w: 800, h: 600 })
    const [graphData, setGraphData] = useState({ nodes: [], links: [] })
    const [selectedNode, setSelectedNode] = useState(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)
    const [settled, setSettled] = useState(false)

    // ── responsive sizing ─────────────────────────────────────────────────
    useEffect(() => {
        if (!containerRef.current) return
        const ro = new ResizeObserver(([e]) =>
            setDims({ w: e.contentRect.width, h: e.contentRect.height })
        )
        ro.observe(containerRef.current)
        return () => ro.disconnect()
    }, [])

    // ── load graph data ───────────────────────────────────────────────────
    useEffect(() => {
        axios.get(`${import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}/graph`)
            .then(({ data }) => {
                const nodes = data.nodes.map(n => ({
                    id: n.id,
                    name: n.label,
                    nodeType: n.type,
                    color: NODE_COLORS[n.type] || '#94a3b8',
                    val: NODE_SIZE[n.type] || 5,
                    nodeData: n.data,
                }))

                const seen = new Set()
                const links = data.edges
                    .filter(e => {
                        const k = `${e.source}||${e.target}`
                        if (seen.has(k)) return false
                        seen.add(k)
                        return true
                    })
                    .map(e => ({
                        source: e.source,
                        target: e.target,
                        relation: e.relation,
                    }))

                // Calculate connection counts for each node
                const connectionCounts = {}
                links.forEach(link => {
                    connectionCounts[link.source] = (connectionCounts[link.source] || 0) + 1
                    connectionCounts[link.target] = (connectionCounts[link.target] || 0) + 1
                })

                nodes.forEach(n => {
                    n.connections = connectionCounts[n.id] || 0
                })

                setGraphData({ nodes, links })
                setLoading(false)
            })
            .catch(err => {
                console.error(err)
                setError('Failed to load graph. Is the backend running?')
                setLoading(false)
            })
    }, [])

    // ── tune forces after mount ───────────────────────────────────────────
    useEffect(() => {
        if (!fgRef.current || loading) return
        // Gentler repulsion so the graph doesn't explode
        fgRef.current.d3Force('charge')?.strength(-80)
        // Moderate link distance
        fgRef.current.d3Force('link')?.distance(60)
    }, [loading])

    // ── highlights ────────────────────────────────────────────────────────
    const hlSet = new Set(highlightedIds)

    const nodeColor = useCallback(node => {
        if (!hlSet.size) return node.color
        // if highlighted, return bright yellow. Otherwise very dim version of natural color
        return hlSet.has(node.id) ? '#ffd700' : node.color + '20'
    }, [highlightedIds])

    const linkColor = useCallback(link => {
        if (!hlSet.size) return 'rgba(148, 163, 184, 0.15)'
        const s = typeof link.source === 'object' ? link.source.id : link.source
        const t = typeof link.target === 'object' ? link.target.id : link.target
        if (hlSet.has(s) && hlSet.has(t)) return 'rgba(255,215,0,0.9)'
        if (hlSet.has(s) || hlSet.has(t)) return 'rgba(255,215,0,0.3)'
        return 'rgba(148, 163, 184, 0.05)'
    }, [highlightedIds])

    const linkWidth = useCallback(link => {
        if (!hlSet.size) return 0.5
        const s = typeof link.source === 'object' ? link.source.id : link.source
        const t = typeof link.target === 'object' ? link.target.id : link.target
        return hlSet.has(s) && hlSet.has(t) ? 3 : 0.5
    }, [highlightedIds])

    // ── zoom on highlight ──────────────────────────────────────────────────
    useEffect(() => {
        if (!fgRef.current || loading || !graphData.nodes.length || highlightedIds.length === 0) return;

        // Find the coordinates of all highlighted nodes
        const hlNodes = graphData.nodes.filter(n => hlSet.has(n.id));
        if (hlNodes.length === 0) return;

        let totalX = 0, totalY = 0, totalZ = 0;
        let validNodes = 0;

        hlNodes.forEach(node => {
            if (node.x !== undefined && node.y !== undefined && node.z !== undefined) {
                totalX += node.x;
                totalY += node.y;
                totalZ += node.z;
                validNodes++;
            }
        });

        if (validNodes === 0) return;

        const centerX = totalX / validNodes;
        const centerY = totalY / validNodes;
        const centerZ = totalZ / validNodes;

        // Distance from camera scales with the number of nodes we need to fit in view
        // More nodes = zoom out more. 
        const cameraZOffset = Math.max(200, validNodes * 20);

        fgRef.current.cameraPosition(
            { x: centerX, y: centerY, z: centerZ + cameraZOffset },
            { x: centerX, y: centerY, z: centerZ },
            1200 // smooth transition ms
        );
    }, [highlightedIds, loading, graphData.nodes]);

    // ── node click ────────────────────────────────────────────────────────
    const handleNodeClick = useCallback(node => {
        setSelectedNode(node)
        // Move camera to node smoothly without pausing the engine manually
        if (fgRef.current && node.x !== undefined && node.y !== undefined) {
            fgRef.current.cameraPosition(
                { x: node.x, y: node.y, z: (node.z || 0) + 180 },
                { x: node.x, y: node.y, z: node.z || 0 },
                1000
            )
        }
    }, [])

    return (
        <div
            ref={containerRef}
            style={{ width: '100%', height: '100%', position: 'relative', background: '#f8fafc', overflow: 'hidden' }}
        >
            {/* Loading */}
            {loading && (
                <div style={styles.centered}>
                    <div style={styles.spinner} />
                    <span style={{ color: '#64748b', fontSize: 13, marginTop: 12 }}>Building graph…</span>
                </div>
            )}

            {/* Error */}
            {error && (
                <div style={styles.centered}>
                    <span style={{ color: '#ef4444', fontSize: 13 }}>{error}</span>
                </div>
            )}

            {/* Graph */}
            {!loading && !error && dims.w > 0 && (
                <ForceGraph3D
                    ref={fgRef}
                    graphData={graphData}
                    width={dims.w}
                    height={dims.h}
                    backgroundColor="#f8fafc"
                    showNavInfo={false}
                    enableNavigationControls={true}
                    enableNodeDrag={true}

                    // Tooltip on hover
                    nodeLabel={node =>
                        `<div style="background:rgba(255,255,255,0.95);border:1px solid ${NODE_COLORS[node.nodeType] || '#cbd5e1'};border-radius:8px;padding:8px 12px;font-family:system-ui,sans-serif;pointer-events:none;box-shadow:0 4px 6px -1px rgba(0,0,0,0.1)">
                            <div style="font-size:10px;color:${NODE_COLORS[node.nodeType] || '#64748b'};text-transform:uppercase;letter-spacing:.07em;font-weight:700;margin-bottom:3px">${node.nodeType}</div>
                            <div style="font-size:12px;font-weight:600;color:#0f172a">${node.name}</div>
                        </div>`
                    }

                    nodeColor={nodeColor}
                    nodeVal="val"
                    nodeOpacity={1}
                    nodeResolution={16}

                    linkColor={linkColor}
                    linkWidth={linkWidth}
                    linkOpacity={1}
                    linkDirectionalParticles={1}
                    linkDirectionalParticleWidth={0.8}
                    linkDirectionalParticleSpeed={0.004}

                    // ── Physics ──────────────────────────────────────
                    // warmupTicks: pre-run physics before first paint → no exploding start
                    warmupTicks={120}
                    // Stop simulation after 8 s or 400 frames, whichever first
                    cooldownTime={8000}
                    cooldownTicks={400}
                    // d3 defaults: alphaDecay=0.0228, velocityDecay=0.4
                    // Slight increase to settle faster with less chaos
                    d3AlphaDecay={0.025}
                    d3VelocityDecay={0.45}

                    onNodeClick={handleNodeClick}
                    onBackgroundClick={() => setSelectedNode(null)}
                />
            )}

            {/* ── Legend ── */}
            <div style={styles.legend}>
                <div style={styles.legendTitle}>Node Types</div>
                {Object.entries(NODE_COLORS).map(([type, color]) => (
                    <div key={type} style={styles.legendRow}>
                        <div style={{ ...styles.dot, background: color }} />
                        <span style={styles.legendLabel}>{type}</span>
                    </div>
                ))}
                <div style={{ ...styles.legendTitle, marginTop: 10 }}>
                    {graphData.nodes.length} nodes · {graphData.links.length} edges
                </div>
            </div>

            {/* ── Node info panel ── */}
            {selectedNode && (
                <div style={styles.infoPanel(NODE_COLORS[selectedNode.nodeType])}>
                    {/* Header */}
                    <div style={styles.infoPanelHeader}>
                        <div>
                            <div style={styles.infoPanelName}>{selectedNode.nodeType}</div>
                        </div>
                        <button
                            onClick={() => setSelectedNode(null)}
                            style={styles.closeBtn}
                        >✕</button>
                    </div>

                    {/* Fields */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 12 }}>
                        {selectedNode.nodeData && Object.entries(selectedNode.nodeData).map(([k, v]) =>
                            v !== null && v !== undefined && String(v).trim() !== '' && (
                                <div key={k} style={styles.fieldRow}>
                                    <span style={styles.fieldKey}>{k}:</span>
                                    <span style={styles.fieldVal}>{String(v)}</span>
                                </div>
                            )
                        )}
                        <div style={styles.fieldRow}>
                            <span style={styles.fieldKey}>Connections:</span>
                            <span style={styles.fieldVal}>{selectedNode.connections || 0}</span>
                        </div>
                    </div>
                    {/* Footnote */}
                    <div style={{ fontSize: 10, color: '#94a3b8', fontStyle: 'italic', borderTop: '1px solid #e2e8f0', paddingTop: 8 }}>
                        Additional fields hidden for readability
                    </div>
                </div>
            )}

            {/* ── Hint bar ── */}
            {!loading && !selectedNode && (
                <div style={styles.hint}>
                    Click node to inspect · Scroll to zoom · Drag to rotate
                </div>
            )}
        </div>
    )
}

// ── Style objects ──────────────────────────────────────────────────────────
const styles = {
    centered: {
        position: 'absolute', inset: 0,
        display: 'flex', flexDirection: 'column',
        alignItems: 'center', justifyContent: 'center',
        zIndex: 10,
    },
    spinner: {
        width: 32, height: 32, borderRadius: '50%',
        border: '3px solid #e2e8f0',
        borderTopColor: '#2563eb',
        animation: 'spin 0.8s linear infinite',
    },
    legend: {
        position: 'absolute', top: 16, left: 16, zIndex: 20,
        background: 'rgba(255,255,255,0.95)',
        border: '1px solid rgba(0,0,0,0.08)',
        borderRadius: 10, padding: '10px 14px',
        backdropFilter: 'blur(8px)',
        pointerEvents: 'none',
        minWidth: 140,
        boxShadow: '0 4px 6px -1px rgba(0,0,0,0.05)',
    },
    legendTitle: {
        fontSize: 10, color: '#64748b',
        textTransform: 'uppercase', letterSpacing: '.1em',
        fontWeight: 700, marginBottom: 7,
    },
    legendRow: {
        display: 'flex', alignItems: 'center', gap: 8, marginBottom: 5,
    },
    dot: {
        width: 10, height: 10, borderRadius: '50%', flexShrink: 0,
    },
    legendLabel: {
        fontSize: 12, color: '#334155', fontWeight: 500,
    },
    infoPanel: (accentColor) => ({
        position: 'absolute', bottom: 'auto', top: 20, left: '50%', transform: 'translateX(-50%)', zIndex: 50,
        background: 'rgba(255,255,255,0.98)',
        border: '1px solid #e2e8f0',
        borderRadius: 8, padding: '16px 20px',
        width: 380, maxHeight: '80vh', overflowY: 'auto',
        boxShadow: '0 10px 25px -5px rgba(0,0,0,0.1), 0 8px 10px -6px rgba(0,0,0,0.1)',
        pointerEvents: 'all',
        backdropFilter: 'blur(12px)',
        fontFamily: 'system-ui, sans-serif',
    }),
    infoPanelHeader: {
        display: 'flex', justifyContent: 'space-between',
        alignItems: 'center',
        paddingBottom: 12, marginBottom: 12,
    },
    infoPanelType: {
        fontSize: 10, textTransform: 'uppercase',
        letterSpacing: '.1em', fontWeight: 700, marginBottom: 3,
    },
    infoPanelName: {
        fontSize: 16, fontWeight: 600, color: '#0f172a', wordBreak: 'break-all',
    },
    closeBtn: {
        color: '#94a3b8', background: 'none', border: 'none',
        cursor: 'pointer', fontSize: 16, lineHeight: 1,
        marginLeft: 8, flexShrink: 0, padding: '4px 8px',
        borderRadius: '4px',
    },
    fieldRow: {
        display: 'flex', gap: 8, fontSize: 12,
    },
    fieldKey: {
        color: '#475569', minWidth: 140, fontWeight: 500, flexShrink: 0,
    },
    fieldVal: {
        color: '#334155', wordBreak: 'break-all',
    },
    hint: {
        position: 'absolute', bottom: 16, left: '50%',
        transform: 'translateX(-50%)',
        zIndex: 20, color: '#475569', fontSize: 12, fontWeight: 500,
        background: 'rgba(255,255,255,0.9)', border: '1px solid #e2e8f0', borderRadius: 20,
        padding: '6px 16px', pointerEvents: 'none',
        backdropFilter: 'blur(4px)', whiteSpace: 'nowrap',
        boxShadow: '0 2px 4px rgba(0,0,0,0.05)',
    },
}