import { useState, useRef, useEffect } from 'react'
import axios from 'axios'

const STARTER_CHIPS = [
    'Which products have the most billing documents?',
    'Show orders with incomplete O2C flows',
    'Trace billing document 90504248',
    'Which customer has the highest total order value?',
]

export default function ChatPanel({ onHighlight, activeHighlightIndex, setActiveHighlightIndex }) {
    const [messages, setMessages] = useState([])
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)
    const [expandedSql, setExpandedSql] = useState(new Set())
    const [copiedIndex, setCopiedIndex] = useState(null)
    const messagesEndRef = useRef(null)

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }

    useEffect(() => {
        scrollToBottom()
    }, [messages])

    const buildHighlightIds = (referencedIds) => {
        const ids = []
        if (referencedIds.customers) {
            ids.push(...referencedIds.customers)
        }
        if (referencedIds.sales_orders) {
            ids.push(...referencedIds.sales_orders.map((id) => `SO-${id}`))
        }
        if (referencedIds.deliveries) {
            ids.push(...referencedIds.deliveries.map((id) => `DE-${id}`))
        }
        if (referencedIds.billing_documents) {
            ids.push(...referencedIds.billing_documents.map((id) => `BD-${id}`))
        }
        if (referencedIds.journal_entries) {
            ids.push(...referencedIds.journal_entries.map((id) => `JE-${id}-1`))
        }
        if (referencedIds.payments) {
            ids.push(...referencedIds.payments.map((id) => `PY-${id}`))
        }
        return ids
    }

    const handleSendMessage = async (text = null) => {
        const messageText = text || input.trim()
        if (!messageText) return

        const userMessage = { role: 'user', content: messageText }
        const newMessages = [...messages, userMessage]
        setMessages(newMessages)
        setInput('')
        setLoading(true)

        try {
            const response = await axios.post('http://localhost:8000/chat', {
                message: messageText,
                history: messages,
            })

            const { answer, sql, explanation, data, referenced_ids, row_count } = response.data

            const assistantMessage = {
                role: 'assistant',
                content: answer,
                sql,
                explanation,
                data,
                referenced_ids,
                row_count,
            }

            setMessages([...newMessages, assistantMessage])

            if (referenced_ids) {
                const highlightIds = buildHighlightIds(referenced_ids)
                onHighlight(highlightIds)
                setActiveHighlightIndex(newMessages.length) // newMessages.length is the index of the new assistantMessage
            }
        } catch (error) {
            console.error('Error sending message:', error)
            const errorMessage = {
                role: 'assistant',
                content: 'Error: Could not get response from server.',
            }
            setMessages([...newMessages, errorMessage])
        } finally {
            setLoading(false)
        }
    }

    const handleChipClick = (chip) => {
        handleSendMessage(chip)
    }

    const toggleSql = (msgIndex) => {
        const newExpanded = new Set(expandedSql)
        if (newExpanded.has(msgIndex)) {
            newExpanded.delete(msgIndex)
        } else {
            newExpanded.add(msgIndex)
        }
        setExpandedSql(newExpanded)
    }

    const handleCopyTable = (data, idx) => {
        if (!data || data.length === 0) return
        const headers = Object.keys(data[0])
        const tsv = [
            headers.join('\t'),
            ...data.map(row => headers.map(h => String(row[h] ?? '')).join('\t'))
        ].join('\n')

        navigator.clipboard.writeText(tsv).then(() => {
            setCopiedIndex(idx)
            setTimeout(() => setCopiedIndex(null), 2000)
        })
    }

    const toggleHighlight = (msg, index) => {
        if (activeHighlightIndex === index) {
            // Turn off current highlight
            setActiveHighlightIndex(null)
            onHighlight([])
        } else {
            // Set this message's ids as the current highlight
            setActiveHighlightIndex(index)
            if (msg.referenced_ids) {
                const highlightIds = buildHighlightIds(msg.referenced_ids)
                onHighlight(highlightIds)
            } else {
                onHighlight([])
            }
        }
    }

    const showChips = messages.length === 0

    return (
        <div className="flex flex-col h-full bg-white relative">

            {/* Header */}
            <div className="px-5 py-4 border-b border-gray-100 flex flex-col">
                <span className="text-sm font-semibold text-gray-900">Chat with Graph</span>
                <span className="text-xs text-gray-500">Order to Cash</span>
            </div>

            {/* Messages area */}
            <div className="flex-1 overflow-y-auto p-5 space-y-6 min-h-0 bg-gray-50/50">

                {messages.length === 0 && (
                    <div className="flex items-start gap-3">
                        <div className="w-8 h-8 rounded bg-gray-900 flex-shrink-0 flex items-center justify-center text-white font-bold text-sm">
                            D
                        </div>
                        <div>
                            <div className="font-semibold text-sm text-gray-900">Dodge AI</div>
                            <div className="text-xs text-gray-500 mb-2">Graph Agent</div>
                            <div className="text-sm text-gray-800 bg-white p-3 border border-gray-200 rounded-lg shadow-sm">
                                Hi! I can help you analyze the <b>Order to Cash</b> process.
                            </div>
                        </div>
                    </div>
                )}
                {showChips && (
                    <div className="flex flex-wrap gap-2">
                        {STARTER_CHIPS.map((chip, i) => (
                            <button
                                key={i}
                                onClick={() => handleChipClick(chip)}
                                className="text-xs bg-indigo-50 border border-indigo-100 rounded-full px-4 py-2 hover:bg-indigo-100 hover:border-indigo-200 cursor-pointer text-indigo-700 transition-all shadow-sm flex-shrink-0"
                            >
                                {chip}
                            </button>
                        ))}
                    </div>
                )}

                {messages.map((msg, i) => (
                    <div
                        key={i}
                        className={`flex gap-3 w-full ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                    >
                        {msg.role === 'assistant' && (
                            <div className="w-8 h-8 rounded bg-gray-900 flex-shrink-0 flex items-center justify-center text-white font-bold text-sm mt-1">
                                D
                            </div>
                        )}
                        <div className={`max-w-[85%] min-w-0 flex flex-col ${msg.role === 'user' ? 'items-end' : 'items-start'}`}>
                            {msg.role === 'user' && <span className="text-xs font-semibold text-gray-900 mb-1 flex items-center gap-2">You <div className="w-6 h-6 rounded-full bg-gray-300"></div></span>}
                            {msg.role === 'assistant' && (
                                <div className="mb-1">
                                    <span className="text-sm font-semibold text-gray-900 block">Dodge AI</span>
                                    <span className="text-xs text-gray-500">Graph Agent</span>
                                </div>
                            )}
                            <div
                                className={`rounded-xl px-4 py-3 shadow-sm text-sm whitespace-pre-wrap leading-relaxed ${msg.role === 'user'
                                    ? 'bg-gray-900 text-white rounded-tr-sm'
                                    : 'bg-white text-gray-800 border border-gray-200 w-full'
                                    }`}
                            >
                                {msg.content}
                            </div>

                            {msg.role === 'assistant' && msg.referenced_ids && (
                                <div className="mt-3 flex justify-start">
                                    <button
                                        onClick={() => toggleHighlight(msg, i)}
                                        className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold shadow-sm transition-all border cursor-pointer ${activeHighlightIndex === i
                                            ? 'bg-yellow-100 text-yellow-800 border-yellow-300 ring-1 ring-yellow-400'
                                            : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'
                                            }`}
                                    >
                                        <span className={`w-2 h-2 rounded-full ${activeHighlightIndex === i ? 'bg-yellow-500 animate-pulse' : 'bg-gray-400'}`}></span>
                                        {activeHighlightIndex === i ? 'Graph Highlight Active' : 'Highlight on Graph'}
                                    </button>
                                </div>
                            )}

                            {msg.sql && (
                                <div className="mt-3 text-xs bg-gray-50 rounded-xl p-2 border border-gray-200">
                                    <button
                                        onClick={() => toggleSql(i)}
                                        className="text-gray-600 font-medium hover:text-gray-900 flex items-center gap-1 cursor-pointer w-full text-left px-1"
                                    >
                                        <span className="text-lg leading-none">{expandedSql.has(i) ? '▾' : '▸'}</span>
                                        {expandedSql.has(i) ? 'Hide SQL Code' : 'View SQL Code'}
                                    </button>
                                    {expandedSql.has(i) && (
                                        <div className="mt-2 bg-gray-900 text-gray-100 rounded-md p-3 overflow-x-auto max-h-48 overflow-y-auto shadow-inner">
                                            <code className="font-mono text-xs whitespace-pre">{msg.sql}</code>
                                        </div>
                                    )}
                                </div>
                            )}

                            {msg.data && msg.data.length > 0 && (
                                <div className="mt-4 text-xs bg-gray-50 p-3 rounded-xl border border-gray-200 w-full overflow-hidden">
                                    <div className="flex items-center justify-between text-gray-700 mb-3">
                                        <span className="font-semibold">Results ({msg.row_count} row{msg.row_count !== 1 ? 's' : ''})</span>
                                        <button
                                            onClick={() => handleCopyTable(msg.data, i)}
                                            className="px-3 py-1.5 bg-white border border-gray-300 rounded-md hover:bg-gray-100 transition shadow-sm text-xs font-medium cursor-pointer flex items-center gap-1"
                                        >
                                            {copiedIndex === i ? (
                                                <span className="text-green-600">✓ Copied</span>
                                            ) : (
                                                <span>📋 Copy Table</span>
                                            )}
                                        </button>
                                    </div>
                                    <div className="overflow-x-auto max-h-64 rounded bg-white border border-gray-200 shadow-inner">
                                        <table className="w-full border-collapse text-xs divide-y divide-gray-200">
                                            <thead className="bg-gray-100 sticky top-0 shadow-sm">
                                                <tr>
                                                    {Object.keys(msg.data[0]).map((key) => (
                                                        <th
                                                            key={key}
                                                            className="px-3 py-2 text-left font-semibold text-gray-700 whitespace-nowrap"
                                                        >
                                                            {key}
                                                        </th>
                                                    ))}
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-gray-200">
                                                {msg.data.slice(0, 10).map((row, rowIdx) => (
                                                    <tr key={rowIdx} className="hover:bg-blue-50 transition-colors">
                                                        {Object.values(row).map((val, valIdx) => (
                                                            <td
                                                                key={valIdx}
                                                                className="px-3 py-2 text-gray-800 max-w-[150px] truncate"
                                                                title={String(val)}
                                                            >
                                                                {val === null || val === undefined
                                                                    ? <span className="text-gray-400 italic">null</span>
                                                                    : String(val)}
                                                            </td>
                                                        ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                    {msg.row_count > 10 && (
                                        <div className="text-center mt-2 text-gray-500 italic">
                                            Showing first 10 rows. Use "Copy Table" to get all results.
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    </div>
                ))}

                {loading && (
                    <div className="flex gap-3 justify-start w-full">
                        <div className="w-8 h-8 rounded bg-gray-900 flex-shrink-0 flex items-center justify-center text-white font-bold text-sm mt-1">
                            D
                        </div>
                        <div className="flex flex-col items-start pt-2">
                            <div className="flex gap-1.5 p-3 bg-white border border-gray-200 shadow-sm rounded-xl">
                                <div className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"></div>
                                <div
                                    className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"
                                    style={{ animationDelay: '0.15s' }}
                                ></div>
                                <div
                                    className="w-1.5 h-1.5 bg-gray-400 rounded-full animate-bounce"
                                    style={{ animationDelay: '0.3s' }}
                                ></div>
                            </div>
                        </div>
                    </div>
                )}

                <div ref={messagesEndRef} />
            </div>

            {/* Input area */}
            <div className="flex-shrink-0 p-4 bg-white shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.05)] border-t border-gray-100">
                <form
                    className="flex flex-col gap-2 bg-gray-50 border border-gray-200 rounded-xl p-3 shadow-inner"
                    onSubmit={(e) => {
                        e.preventDefault()
                        handleSendMessage()
                    }}
                >
                    <div className="flex items-center gap-2 mb-1 px-1">
                        <span className="w-2 h-2 rounded-full bg-green-500"></span>
                        <span className="text-xs text-gray-500 font-medium">{loading ? 'Dodge AI is analyzing...' : 'Dodge AI is awaiting instructions'}</span>
                    </div>
                    <div className="flex gap-2">
                        <input
                            type="text"
                            placeholder="Analyze anything"
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            disabled={loading}
                            className="flex-1 bg-transparent px-2 text-sm focus:outline-none text-gray-800 disabled:opacity-50"
                        />
                        <button
                            type="submit"
                            disabled={loading || !input.trim()}
                            className="bg-gray-500 hover:bg-gray-600 text-white px-5 py-1.5 rounded-lg disabled:opacity-40 disabled:cursor-not-allowed text-sm font-medium transition-colors"
                        >
                            Send
                        </button>
                    </div>
                </form>
            </div>
        </div>
    )
}
