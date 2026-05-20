import { useMemo } from "react";
import { graphql, useLazyLoadQuery } from "react-relay";

import type { SpanCumulativeTokenCountDetailsQuery } from "./__generated__/SpanCumulativeTokenCountDetailsQuery.graphql";
import { TokenCountDetails } from "./TokenCountDetails";

export function SpanCumulativeTokenCountDetails(props: { spanNodeId: string }) {
  const data = useLazyLoadQuery<SpanCumulativeTokenCountDetailsQuery>(
    graphql`
      query SpanCumulativeTokenCountDetailsQuery($nodeId: ID!) {
        node(id: $nodeId) {
          __typename
          ... on Span {
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
    `,
    { nodeId: props.spanNodeId }
  );

  const tokenData = useMemo(() => {
    if (data.node.__typename === "Span") {
      const prompt = data.node.cumulativeTokenCountPrompt ?? 0;
      const completion = data.node.cumulativeTokenCountCompletion ?? 0;
      const total = data.node.cumulativeTokenCountTotal ?? 0;
      const promptDetails: Record<string, number> = {};
      const completionDetails: Record<string, number> = {};
      if (data.node.cumulativeTokenPromptDetails?.audio) {
        promptDetails.audio = data.node.cumulativeTokenPromptDetails.audio;
      }
      if (data.node.cumulativeTokenPromptDetails?.cacheRead) {
        promptDetails["cache read"] =
          data.node.cumulativeTokenPromptDetails.cacheRead;
      }
      if (data.node.cumulativeTokenPromptDetails?.cacheWrite) {
        promptDetails["cache write"] =
          data.node.cumulativeTokenPromptDetails.cacheWrite;
      }
      if (data.node.cumulativeTokenCompletionDetails?.reasoning) {
        completionDetails.reasoning =
          data.node.cumulativeTokenCompletionDetails.reasoning;
      }
      if (data.node.cumulativeTokenCompletionDetails?.audio) {
        completionDetails.audio =
          data.node.cumulativeTokenCompletionDetails.audio;
      }
      return {
        total,
        prompt,
        completion,
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
