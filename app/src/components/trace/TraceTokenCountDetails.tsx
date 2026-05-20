import { useMemo } from "react";
import { graphql, useLazyLoadQuery } from "react-relay";

import type { TraceTokenCountDetailsQuery } from "./__generated__/TraceTokenCountDetailsQuery.graphql";
import { TokenCountDetails } from "./TokenCountDetails";

export function TraceTokenCountDetails(props: { traceNodeId: string }) {
  const data = useLazyLoadQuery<TraceTokenCountDetailsQuery>(
    graphql`
      query TraceTokenCountDetailsQuery($nodeId: ID!) {
        node(id: $nodeId) {
          __typename
          ... on Trace {
            rootSpan {
              cumulativeTokenCountTotal
              cumulativeTokenCountPrompt
              cumulativeTokenCountCompletion
              cumulativeTokenPromptDetails {
                audio
                cacheRead
                cacheWrite
              }
              cumulativeTokenCompletionDetails {
                reasoning
                audio
              }
            }
          }
        }
      }
    `,
    { nodeId: props.traceNodeId }
  );

  const tokenData = useMemo(() => {
    if (data.node.__typename === "Trace") {
      const tracePrompt = data.node.rootSpan?.cumulativeTokenCountPrompt ?? 0;
      const traceCompletion =
        data.node.rootSpan?.cumulativeTokenCountCompletion ?? 0;
      const traceTotal = data.node.rootSpan?.cumulativeTokenCountTotal ?? 0;
      const promptDetails: Record<string, number> = {};
      const completionDetails: Record<string, number> = {};
      if (data.node.rootSpan?.cumulativeTokenPromptDetails?.audio) {
        promptDetails.audio =
          data.node.rootSpan.cumulativeTokenPromptDetails.audio;
      }
      if (data.node.rootSpan?.cumulativeTokenPromptDetails?.cacheRead) {
        promptDetails["cache read"] =
          data.node.rootSpan.cumulativeTokenPromptDetails.cacheRead;
      }
      if (data.node.rootSpan?.cumulativeTokenPromptDetails?.cacheWrite) {
        promptDetails["cache write"] =
          data.node.rootSpan.cumulativeTokenPromptDetails.cacheWrite;
      }
      if (data.node.rootSpan?.cumulativeTokenCompletionDetails?.reasoning) {
        completionDetails.reasoning =
          data.node.rootSpan.cumulativeTokenCompletionDetails.reasoning;
      }
      if (data.node.rootSpan?.cumulativeTokenCompletionDetails?.audio) {
        completionDetails.audio =
          data.node.rootSpan.cumulativeTokenCompletionDetails.audio;
      }
      return {
        total: traceTotal,
        prompt: tracePrompt,
        completion: traceCompletion,
        promptDetails:
          Object.keys(promptDetails).length > 0 ? promptDetails : undefined,
        completionDetails:
          Object.keys(completionDetails).length > 0
            ? completionDetails
            : undefined,
      };
    }

    return {
      total: null,
      prompt: null,
      completion: null,
    };
  }, [data.node]);

  return <TokenCountDetails {...tokenData} />;
}
