export function useBlockList(blocksRef, createBlockFn) {
  function addBlock(typeCode, index) {
    const block = createBlockFn(typeCode)
    if (index !== undefined) {
      blocksRef.value.splice(index + 1, 0, block)
    } else {
      blocksRef.value.push(block)
    }
  }

  function removeBlock(index) {
    blocksRef.value.splice(index, 1)
  }

  function cloneBlock(index) {
    const block = blocksRef.value[index]
    const copy = JSON.parse(JSON.stringify(block))
    copy.id = 'b' + Math.round(Math.random() * 100000000)
    blocksRef.value.splice(index + 1, 0, copy)
  }

  function moveToTop(index) {
    if (index > 0) {
      blocksRef.value.splice(0, 0, blocksRef.value.splice(index, 1)[0])
    }
  }

  function toggleBlock(index) {
    blocksRef.value[index].minimized = !blocksRef.value[index].minimized
  }

  return { addBlock, removeBlock, cloneBlock, moveToTop, toggleBlock }
}
