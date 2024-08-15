import { parseArgs } from "util";

export const { values } = parseArgs({
    args: Bun.argv,
    options: {
        opening: {
            type: 'string',
        },
    },
    strict: true,
});

