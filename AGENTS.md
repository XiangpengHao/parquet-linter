## Engineering principles

1. Think about minimal changes to complete the task.
2. Always cargo check after coding.
3. Consider refactoring when a function needs more than 3 args.
4. Whenever writing a if or edge case condition, (1) think why it is needed here, whether it is a real edge case, (2) think whether we can move the condition upper to the caller, i.e., is this the best place to handle edge case?

## Testing

1. All test cases are intended for human to read. They are part of the code and documents.
2. Test cases should be short, concise, yet have high coverage. Writing test cases almost requires highest level of thinking.
