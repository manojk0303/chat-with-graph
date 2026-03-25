import { useState, useRef, useEffect, useCallback } from 'react'
import GraphView from './components/GraphView'
import ChatPanel from './components/ChatPanel'

export default function App() {
    const [highlightedIds, setHighlightedIds] = useState([])
    const [activeHighlightIndex, setActiveHighlightIndex] = useState(null)
    const [chatWidth, setChatWidth] = useState(35) // Start at 35% width
    const containerRef = useRef(null)
    const isDragging = useRef(false)

    const handleMouseDown = useCallback((e) => {
        e.preventDefault()
        isDragging.current = true
        document.body.style.cursor = 'col-resize'
        document.body.style.userSelect = 'none'
    }, [])

    const handleMouseMove = useCallback((e) => {
        if (!isDragging.current || !containerRef.current) return

        const containerRect = containerRef.current.getBoundingClientRect()
        let newChatWidth = ((containerRect.right - e.clientX) / containerRect.width) * 100

        // Clamp width between 20% and 80%
        newChatWidth = Math.max(20, Math.min(newChatWidth, 80))
        setChatWidth(newChatWidth)
    }, [])

    const handleMouseUp = useCallback(() => {
        if (isDragging.current) {
            isDragging.current = false
            document.body.style.cursor = 'default'
            document.body.style.userSelect = 'auto'
        }
    }, [])

    useEffect(() => {
        document.addEventListener('mousemove', handleMouseMove)
        document.addEventListener('mouseup', handleMouseUp)
        return () => {
            document.removeEventListener('mousemove', handleMouseMove)
            document.removeEventListener('mouseup', handleMouseUp)
        }
    }, [handleMouseMove, handleMouseUp])

    const handleClearHighlights = () => {
        setHighlightedIds([])
        setActiveHighlightIndex(null)
    }

    return (
        <div className="flex flex-col h-screen overflow-hidden font-sans">
            {/* Header */}
            <div className="flex-shrink-0 h-16 bg-white border-b border-gray-200 px-6 flex items-center justify-between shadow-sm z-10 relative">
                <div className="flex items-center gap-3">
                    <h1 className="text-lg font-medium text-gray-400">Order to Cash</h1>
                </div>
                {highlightedIds.length > 0 && (
                    <button
                        onClick={handleClearHighlights}
                        className="text-sm font-medium bg-gray-100 hover:bg-gray-200 text-gray-800 border border-gray-300 px-4 py-1.5 rounded-md transition-colors cursor-pointer flex items-center gap-2"
                    >
                        ✕ Clear Filters
                    </button>
                )}
            </div>

            {/* Main content */}
            <div className="flex flex-1 min-h-0 overflow-hidden bg-gray-50 p-2">
                <div ref={containerRef} className="flex flex-1 rounded-xl overflow-hidden border border-gray-200 shadow-sm bg-white" style={{ display: 'flex', flexDirection: 'row' }}>
                    {/* Left panel - Graph */}
                    <div className="h-full relative shrink-0" style={{ width: `${100 - chatWidth}%`, position: 'relative' }}>
                        <GraphView highlightedIds={highlightedIds} />
                    </div>

                    {/* Drag Handle */}
                    <div
                        onMouseDown={handleMouseDown}
                        className="w-1.5 hover:w-2 bg-gray-100 border-x border-gray-200 hover:bg-blue-400 cursor-col-resize shrink-0 transition-all z-20 relative flex items-center justify-center group"
                    >
                        <div className="flex flex-col gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                            <div className="w-0.5 h-1 bg-white rounded-full"></div>
                            <div className="w-0.5 h-1 bg-white rounded-full"></div>
                            <div className="w-0.5 h-1 bg-white rounded-full"></div>
                        </div>
                    </div>

                    {/* Right panel - Chat */}
                    <div className="h-full flex flex-col shrink-0 bg-white" style={{ width: `calc(${chatWidth}% - 6px)` }}>
                        <ChatPanel
                            onHighlight={setHighlightedIds}
                            activeHighlightIndex={activeHighlightIndex}
                            setActiveHighlightIndex={setActiveHighlightIndex}
                        />
                    </div>
                </div>
            </div>
        </div >
    )
}
